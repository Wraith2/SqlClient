// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;

namespace Microsoft.Data.SqlClient.SNI
{
    /// <summary>
    /// SNI MARS connection. Multiple MARS streams will be overlaid on this connection.
    /// </summary>
    internal class SNIMarsConnection
    {
        private readonly object _sync;
        private readonly Guid _connectionId;
        private readonly Dictionary<int, SNIMarsHandle> _sessions;
        private SNIHandle _lowerHandle;
        private int _nextSessionId;

        private QueuedTaskScheduler _scheduler;
        private TaskFactory _factory;
        /// <summary>
        /// Connection ID
        /// </summary>
        public Guid ConnectionId => _connectionId;

        public int ProtocolVersion => _lowerHandle.ProtocolVersion;

        public object DemuxerSync => _sync;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="lowerHandle">Lower handle</param>
        public SNIMarsConnection(SNIHandle lowerHandle)
        {
            _sync = new object();
            _connectionId = Guid.NewGuid();
            _sessions = new Dictionary<int, SNIMarsHandle>();
            _demuxState = DemuxState.Header;
            _headerCount = 0;
            _headerBytes = new byte[SNISMUXHeader.HEADER_LENGTH];
            _nextSessionId = -1;
            _lowerHandle = lowerHandle;
            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "Created MARS Session Id {0}", args0: ConnectionId);
            _lowerHandle.SetAsyncCallbacks(HandleReceiveComplete, HandleSendComplete);
        }

        public SNIMarsHandle CreateMarsSession(object callbackObject, bool async)
        {
            lock (DemuxerSync)
            {
                ushort sessionId = unchecked ( (ushort)(Interlocked.Increment(ref _nextSessionId) % ushort.MaxValue) );

                SNIMarsHandle handle = new SNIMarsHandle(this, sessionId, callbackObject, async);
                _sessions.Add(sessionId, handle);
                SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, SNI MARS Handle Id {1}, created new MARS Session {2}", args0: ConnectionId, args1: handle?.ConnectionId, args2: sessionId);
                return handle;
            }
        }

        /// <summary>
        /// Start receiving
        /// </summary>
        /// <returns></returns>
        public uint StartReceive()
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                if (LocalAppContextSwitches.UseExperimentalMARSThreading)
                {
                    if (_scheduler == null)
                    {
                        _scheduler = new QueuedTaskScheduler(1, $"MARSIOScheduler_{_connectionId}", false, ThreadPriority.Normal);
                        _factory = new TaskFactory(_scheduler);
                    }
                    // will start an async task on the scheduler and immediatley return so this await is safe
                    return _factory.StartNew(StartAsyncReceiveLoopForConnection, this).GetAwaiter().GetResult();
                }
                else
                {
                    return StartAsyncReceiveLoopForConnection(this);
                }
            }

            static uint StartAsyncReceiveLoopForConnection(object state)
            {
                SNIMarsConnection connection = (SNIMarsConnection)state;
                SNIPacket packet = null;

                if (connection.ReceiveAsync(ref packet) == TdsEnums.SNI_SUCCESS_IO_PENDING)
                {
                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, Success IO pending.", args0: connection.ConnectionId);
                    return TdsEnums.SNI_SUCCESS_IO_PENDING;
                }
                SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "MARS Session Id {0}, Connection not usable.", args0: connection.ConnectionId);
                return SNICommon.ReportSNIError(SNIProviders.SMUX_PROV, 0, SNICommon.ConnNotUsableError, Strings.SNI_ERROR_19);
            };
        }

        /// <summary>
        /// Send a packet synchronously
        /// </summary>
        /// <param name="packet">SNI packet</param>
        /// <returns>SNI error code</returns>
        public uint Send(SNIPacket packet)
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                lock (DemuxerSync)
                {
                    return _lowerHandle.Send(packet);
                }
            }
        }

        /// <summary>
        /// Send a packet asynchronously
        /// </summary>
        /// <param name="packet">SNI packet</param>
        /// <param name="callback">Completion callback</param>
        /// <returns>SNI error code</returns>
        public uint SendAsync(SNIPacket packet, SNIAsyncCallback callback)
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                lock (DemuxerSync)
                {
                    return _lowerHandle.SendAsync(packet, callback);
                }
            }
        }

        /// <summary>
        /// Receive a packet asynchronously
        /// </summary>
        /// <param name="packet">SNI packet</param>
        /// <returns>SNI error code</returns>
        public uint ReceiveAsync(ref SNIPacket packet)
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                if (packet != null)
                {
                    ReturnPacket(packet);
#if DEBUG
                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, Packet {1} returned", args0: ConnectionId, args1: packet?._id);
#endif
                    packet = null;
                }

                lock (DemuxerSync)
                {
                    var response = _lowerHandle.ReceiveAsync(ref packet);
#if DEBUG
                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, Received new packet {1}", args0: ConnectionId, args1: packet?._id);
#endif
                    return response;
                }
            }
        }

        /// <summary>
        /// Check SNI handle connection
        /// </summary>
        /// <returns>SNI error status</returns>
        public uint CheckConnection()
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                lock (DemuxerSync)
                {
                    return _lowerHandle.CheckConnection();
                }
            }
        }

        /// <summary>
        /// Process a receive error
        /// </summary>
        public void HandleReceiveError(SNIPacket packet)
        {
            Debug.Assert(Monitor.IsEntered(DemuxerSync), "HandleReceiveError was called without demuxer lock being taken.");
            if (!Monitor.IsEntered(DemuxerSync))
            {
                SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "MARS Session Id {0}, function was called without being locked.", args0: ConnectionId);
            }
            foreach (SNIMarsHandle handle in _sessions.Values)
            {
                if (packet.HasCompletionCallback)
                {
                    handle.HandleReceiveError(packet);
#if DEBUG
                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "MARS Session Id {0}, Packet {1} has Completion Callback", args0: ConnectionId, args1: packet?._id);
                }
                else
                {
                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "MARS Session Id {0}, Packet {1} does not have Completion Callback, error not handled.", args0: ConnectionId, args1: packet?._id);
#endif
                }
            }
            Debug.Assert(!packet.IsInvalid, "packet was returned by MarsConnection child, child sessions should not release the packet");
            ReturnPacket(packet);
        }

        /// <summary>
        /// Process a send completion
        /// </summary>
        /// <param name="packet">SNI packet</param>
        /// <param name="sniErrorCode">SNI error code</param>
        public void HandleSendComplete(SNIPacket packet, uint sniErrorCode)
        {
            packet.InvokeCompletionCallback(sniErrorCode);
        }

        /// <summary>
        /// Enable SSL
        /// </summary>
        public uint EnableSsl(uint options)
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                return _lowerHandle.EnableSsl(options);
            }
        }

        /// <summary>
        /// Disable SSL
        /// </summary>
        public void DisableSsl()
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                _lowerHandle.DisableSsl();
            }
        }

        public SNIPacket RentPacket(int headerSize, int dataSize) => _lowerHandle.RentPacket(headerSize, dataSize);

        public void ReturnPacket(SNIPacket packet) => _lowerHandle.ReturnPacket(packet);

#if DEBUG
        /// <summary>
        /// Test handle for killing underlying connection
        /// </summary>
        public void KillConnection()
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                _lowerHandle.KillConnection();
            }
        }
#endif

        private enum DemuxState : uint
        {
            Header = 1,
            Payload = 2,
            Dispatch = 3
        }

        private enum State : uint
        {
            Demux,
            HandleAck,
            HandleData,
            Receive,
            Finish,
            Error
        }


        // the following variables are used only inside HandleRecieveComplete
        // all access to these variables must be performed under lock(DemuxerSync) because
        // RecieveAsync can immediately return a new packet causing reentrant behaviour 
        // without the lock.
        private DemuxState _demuxState;

        private byte[] _headerBytes;
        private int _headerCount;
        private SNISMUXHeader _header;

        private int _payloadLength;
        private int _payloadCount;

        private SNIPacket _partial;

        public void HandleReceiveComplete(SNIPacket packet, uint sniErrorCode)
        {
            using (TrySNIEventScope.Create(nameof(SNIMarsConnection)))
            {
                if (sniErrorCode != TdsEnums.SNI_SUCCESS)
                {
                    lock (DemuxerSync)
                    {
                        HandleReceiveError(packet);
                        SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "MARS Session Id {0}, Handled receive error code: {1}", args0: _lowerHandle?.ConnectionId, args1: sniErrorCode);
                        return;
                    }
                }

                State state = State.Demux;
                State nextState = State.Demux;
                
                SNISMUXHeader handleHeader = default;
                SNIMarsHandle handleSession = null;
                SNIPacket handlePacket = null;

                while (state != State.Error && state != State.Finish)
                {
                    switch (state)
                    {
                        case State.Demux:
                            lock (DemuxerSync)
                            {
                                switch (_demuxState)
                                {
                                    case DemuxState.Header:
                                        int taken = packet.TakeData(_headerBytes, _headerCount, SNISMUXHeader.HEADER_LENGTH - _headerCount);
                                        _headerCount += taken;
                                        SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, took {1} header bytes", args0: _lowerHandle?.ConnectionId, args1: packet.DataLeft, args2: taken);
                                        if (_headerCount == SNISMUXHeader.HEADER_LENGTH)
                                        {
                                            _header.Read(_headerBytes);
                                            _payloadLength = (int)_header.Length;
                                            _payloadCount = 0;
                                            _demuxState = DemuxState.Payload;
                                            state = State.Demux;
                                            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, header complete, _payloadLength {1}", args0: _lowerHandle?.ConnectionId, args1: _payloadLength);
                                            goto case DemuxState.Payload;
                                        }
                                        else
                                        {
                                            state = State.Receive;
                                        }
                                        break;

                                    case DemuxState.Payload:
                                        if (packet.DataLeft == _payloadLength && _partial == null)
                                        {
                                            // if the data in the packet being processed is exactly and only the data that is going to sent
                                            // on to the parser then don't copy it to a new packet just forward the current packet once we've
                                            // fiddled the data pointer so that it skips the header data
                                            _partial = packet;
                                            packet = null;
                                            _partial.SetDataToRemainingContents();
                                            _demuxState = DemuxState.Dispatch;
                                            state = State.Demux;
                                            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, forwarding packet contents", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                                            goto case DemuxState.Dispatch;
                                        }
                                        else
                                        {
                                            if (_partial == null)
                                            {
                                                _partial = RentPacket(headerSize: 0, dataSize: _payloadLength);
                                            }
                                            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, reconstructing packet contents", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                                            int wanted = _payloadLength - _payloadCount;
                                            int transferred = SNIPacket.TransferData(packet, _partial, wanted);
                                            _payloadCount += transferred;
                                            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, took {1} payload bytes", args0: _lowerHandle?.ConnectionId, args1: transferred);

                                            if (_payloadCount == _payloadLength)
                                            {
                                                // payload is complete so dispatch the current packet
                                                _demuxState = DemuxState.Dispatch;
                                                state = State.Receive;
                                                goto case DemuxState.Dispatch;
                                            }
                                            else if (packet.DataLeft == 0)
                                            {
                                                // no more data in the delivered packet so wait for a new one
                                                _demuxState = DemuxState.Payload;
                                                state = State.Receive;
                                            }
                                            else
                                            {
                                                // data left in the delivered packet so start the demux loop 
                                                // again and decode the next packet in the input
                                                _headerCount = 0;
                                                _demuxState = DemuxState.Header;
                                                state = State.Demux;
                                            }
                                        }

                                        break;

                                    case DemuxState.Dispatch:
                                        if (_sessions.TryGetValue(_header.SessionId, out SNIMarsHandle session))
                                        {
                                            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, Current Session assigned to Session Id {1}", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                                            switch ((SNISMUXFlags)_header.Flags)
                                            {
                                                case SNISMUXFlags.SMUX_DATA:
                                                    handleSession = session;
                                                    session = null;
                                                    handleHeader = _header.Clone();
                                                    handlePacket = _partial;
                                                    _partial = null;
                                                    // move to the state for sending the data to the mars handle and setup
                                                    // the state that should be moved to after that operation has succeeded
                                                    state = State.HandleData;
                                                    if (packet != null && packet.DataLeft > 0)
                                                    {
                                                        nextState = State.Demux;
                                                    }
                                                    else
                                                    {
                                                        nextState = State.Receive;
                                                    }
                                                    break;

                                                case SNISMUXFlags.SMUX_ACK:
                                                    handleSession = session;
                                                    session = null;
                                                    handleHeader = _header.Clone();
                                                    ReturnPacket(_partial);
                                                    _partial = null;
                                                    // move to the state for sending the data to the mars handle and setup
                                                    // the state that should be moved to after that operation has succeeded
                                                    state = State.HandleAck;
                                                    if (packet != null && packet.DataLeft > 0)
                                                    {
                                                        nextState = State.Demux;
                                                    }
                                                    else
                                                    {
                                                        nextState = State.Receive;
                                                    }
                                                    break;

                                                case SNISMUXFlags.SMUX_FIN:
                                                    ReturnPacket(_partial);
                                                    _partial = null;
                                                    _sessions.Remove(_header.SessionId);
                                                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "SMUX_FIN | MARS Session Id {0}, SMUX_FIN flag received, Current Header Session Id {1} removed", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                                                    break;

                                                default:
                                                    Debug.Fail("unknown smux packet flag");
                                                    break;
                                            }

                                            // a full packet has been decoded and queued for sending by setting the state or the 
                                            // handle it was sent to no longer exists and the handle has been dropped. Now reset the
                                            // demuxer state ready to recode another packet
                                            _header.Clear();
                                            _headerCount = 0;
                                            _demuxState = DemuxState.Header;

                                            // if the state is set to demux more data and there is no data left then change
                                            // the state to request more data
                                            if (state == State.Demux && (packet == null || packet.DataLeft == 0))
                                            {
                                                if (packet != null)
                                                {
                                                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, run out of data , queuing receive", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                                                }
                                                state = State.Receive;
                                            }

                                        }
                                        else
                                        {
                                            SNILoadHandle.SingletonInstance.LastError = new SNIError(SNIProviders.SMUX_PROV, 0, SNICommon.InvalidParameterError, string.Empty);
                                            HandleReceiveError(packet);
                                            SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "Current Header Session Id {0} not found, MARS Session Id {1} will be destroyed, New SNI error created: {2}", args0: _header.SessionId, args1: _lowerHandle?.ConnectionId, args2: sniErrorCode);
                                            packet = null;
                                            _lowerHandle.Dispose();
                                            _lowerHandle = null;
                                            state = State.Error;
                                        }
                                        break;
                                }
                            }
                            break;

                        case State.HandleAck:
                            Debug.Assert(handleSession != null, "dispatching ack to null SNIMarsHandle");
                            Debug.Assert(!Monitor.IsEntered(DemuxerSync), "do not dispatch ack to session handle while holding the demuxer lock");
                            try
                            {
                                handleSession.HandleAck(handleHeader.Highwater);
                                SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "SMUX_ACK | MARS Session Id {0}, Current Session {1} handled ack", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                            }
                            catch (Exception e)
                            {
                                SNICommon.ReportSNIError(SNIProviders.SMUX_PROV, SNICommon.InternalExceptionError, e);
                            }
                            finally
                            {
                                handleHeader = default;
                                handleSession = null;
                            }
                            state = nextState;
                            nextState = State.Finish;
                            break;

                        case State.HandleData:
                            Debug.Assert(handleSession != null, "dispatching data to null SNIMarsHandle");
                            Debug.Assert(handlePacket != null, "dispatching null data to SNIMarsHandle");
                            Debug.Assert(!Monitor.IsEntered(DemuxerSync), "do not dispatch data to session handle while holding the demuxer lock");
                            // do not ReturnPacket(handlePacket) the receiver is responsible for returning the packet 
                            // once it has been used because it can take sync and async paths from to the receiver and 
                            // only the reciever can make the decision on when it is completed and can be returned
                            try
                            {
                                handleSession.HandleReceiveComplete(handlePacket, handleHeader);
                                SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "SMUX_DATA | MARS Session Id {0}, Current Session {1} completed receiving Data", args0: _lowerHandle?.ConnectionId, args1: _header.SessionId);
                            }
                            finally
                            {
                                handleHeader = default;
                                handleSession = null;
                                handlePacket = null;
                            }
                            state = nextState;
                            nextState = State.Finish;
                            break;

                        case State.Receive:
                            if (packet != null)
                            {
                                Debug.Assert(packet.DataLeft == 0, "loop exit with data remaining");
                                ReturnPacket(packet);
                                packet = null;
                            }

                            lock (DemuxerSync)
                            {
                                uint receiveResult = ReceiveAsync(ref packet);
                                if (receiveResult == TdsEnums.SNI_SUCCESS_IO_PENDING)
                                {
                                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.INFO, "MARS Session Id {0}, SMUX DATA Header SNI Packet received with code {1}", args0: ConnectionId, args1: receiveResult);
                                    packet = null;
                                }
                                else
                                {
                                    HandleReceiveError(packet);
                                    SqlClientEventSource.Log.TrySNITraceEvent(nameof(SNIMarsConnection), EventType.ERR, "MARS Session Id {0}, Handled receive error code: {1}", args0: _lowerHandle?.ConnectionId, args1: receiveResult);
                                }
                            }
                            state = State.Finish;
                            break;
                    }
                }

            }
        }

    }
}

namespace System.Threading.Tasks.Schedulers
{
    /// <summary>
    /// Provides a TaskScheduler that provides control over priorities, fairness, and the underlying threads utilized.
    /// </summary>
    [DebuggerTypeProxy(typeof(QueuedTaskSchedulerDebugView))]
    [DebuggerDisplay("Id={Id}, Queues={DebugQueueCount}, ScheduledTasks = {DebugTaskCount}")]
    public sealed class QueuedTaskScheduler : TaskScheduler, IDisposable
    {
        /// <summary>Debug view for the QueuedTaskScheduler.</summary>
        private class QueuedTaskSchedulerDebugView
        {
            /// <summary>The scheduler.</summary>
            private readonly QueuedTaskScheduler _scheduler;

            /// <summary>Initializes the debug view.</summary>
            /// <param name="scheduler">The scheduler.</param>
            public QueuedTaskSchedulerDebugView(QueuedTaskScheduler scheduler) =>
                _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));

            /// <summary>Gets all of the Tasks queued to the scheduler directly.</summary>
            public IEnumerable<Task> ScheduledTasks
            {
                get
                {
                    var tasks = (_scheduler._targetScheduler != null) ?
                        (IEnumerable<Task>)_scheduler._nonthreadsafeTaskQueue :
                        _scheduler._blockingTaskQueue;
                    return tasks.Where(t => t != null).ToList();
                }
            }

            /// <summary>Gets the prioritized and fair queues.</summary>
            public IEnumerable<TaskScheduler> Queues
            {
                get
                {
                    List<TaskScheduler> queues = new List<TaskScheduler>();
                    foreach (var group in _scheduler._queueGroups)
                        queues.AddRange(group.Value.Cast<TaskScheduler>());
                    return queues;
                }
            }
        }

        /// <summary>
        /// A sorted list of round-robin queue lists.  Tasks with the smallest priority value
        /// are preferred.  Priority groups are round-robin'd through in order of priority.
        /// </summary>
        private readonly SortedList<int, QueueGroup> _queueGroups = new SortedList<int, QueueGroup>();
        /// <summary>Cancellation token used for disposal.</summary>
        private readonly CancellationTokenSource _disposeCancellation = new CancellationTokenSource();
        /// <summary>
        /// The maximum allowed concurrency level of this scheduler.  If custom threads are
        /// used, this represents the number of created threads.
        /// </summary>
        private readonly int _concurrencyLevel;
        /// <summary>Whether we're processing tasks on the current thread.</summary>
        private static readonly ThreadLocal<bool> s_taskProcessingThread = new ThreadLocal<bool>();

        // ***
        // *** For when using a target scheduler
        // ***

        /// <summary>The scheduler onto which actual work is scheduled.</summary>
        private readonly TaskScheduler _targetScheduler;
        /// <summary>The queue of tasks to process when using an underlying target scheduler.</summary>
        private readonly Queue<Task> _nonthreadsafeTaskQueue;
        /// <summary>The number of Tasks that have been queued or that are running whiel using an underlying scheduler.</summary>
        private int _delegatesQueuedOrRunning = 0;

        // ***
        // *** For when using our own threads
        // ***

        /// <summary>The threads used by the scheduler to process work.</summary>
        private readonly Thread[] _threads;
        /// <summary>The collection of tasks to be executed on our custom threads.</summary>
        private readonly BlockingCollection<Task> _blockingTaskQueue;

        // ***

        /// <summary>Initializes the scheduler.</summary>
        public QueuedTaskScheduler() : this(Default, 0) { }

        /// <summary>Initializes the scheduler.</summary>
        /// <param name="targetScheduler">The target underlying scheduler onto which this sceduler's work is queued.</param>
        public QueuedTaskScheduler(TaskScheduler targetScheduler) : this(targetScheduler, 0) { }

        /// <summary>Initializes the scheduler.</summary>
        /// <param name="targetScheduler">The target underlying scheduler onto which this sceduler's work is queued.</param>
        /// <param name="maxConcurrencyLevel">The maximum degree of concurrency allowed for this scheduler's work.</param>
        public QueuedTaskScheduler(
            TaskScheduler targetScheduler,
            int maxConcurrencyLevel)
        {
            if (maxConcurrencyLevel < 0)
                throw new ArgumentOutOfRangeException(nameof(maxConcurrencyLevel));

            // Initialize only those fields relevant to use an underlying scheduler.  We don't
            // initialize the fields relevant to using our own custom threads.
            _targetScheduler = targetScheduler ?? throw new ArgumentNullException("underlyingScheduler");
            _nonthreadsafeTaskQueue = new Queue<Task>();

            // If 0, use the number of logical processors.  But make sure whatever value we pick
            // is not greater than the degree of parallelism allowed by the underlying scheduler.
            _concurrencyLevel = maxConcurrencyLevel != 0 ? maxConcurrencyLevel : Environment.ProcessorCount;
            if (targetScheduler.MaximumConcurrencyLevel > 0 &&
                targetScheduler.MaximumConcurrencyLevel < _concurrencyLevel)
            {
                _concurrencyLevel = targetScheduler.MaximumConcurrencyLevel;
            }
        }

        /// <summary>Initializes the scheduler.</summary>
        /// <param name="threadCount">The number of threads to create and use for processing work items.</param>
        public QueuedTaskScheduler(int threadCount) : this(threadCount, string.Empty, false, ThreadPriority.Normal, ApartmentState.MTA, 0, null, null) { }

        /// <summary>Initializes the scheduler.</summary>
        /// <param name="threadCount">The number of threads to create and use for processing work items.</param>
        /// <param name="threadName">The name to use for each of the created threads.</param>
        /// <param name="useForegroundThreads">A Boolean value that indicates whether to use foreground threads instead of background.</param>
        /// <param name="threadPriority">The priority to assign to each thread.</param>
        /// <param name="threadApartmentState">The apartment state to use for each thread.</param>
        /// <param name="threadMaxStackSize">The stack size to use for each thread.</param>
        /// <param name="threadInit">An initialization routine to run on each thread.</param>
        /// <param name="threadFinally">A finalization routine to run on each thread.</param>
        public QueuedTaskScheduler(
            int threadCount,
            string threadName = "",
            bool useForegroundThreads = false,
            ThreadPriority threadPriority = ThreadPriority.Normal,
            ApartmentState threadApartmentState = ApartmentState.MTA,
            int threadMaxStackSize = 0,
            Action threadInit = null,
            Action threadFinally = null)
        {
            // Validates arguments (some validation is left up to the Thread type itself).
            // If the thread count is 0, default to the number of logical processors.
            if (threadCount < 0)
                throw new ArgumentOutOfRangeException(nameof(threadCount));
            else if (threadCount == 0)
                _concurrencyLevel = Environment.ProcessorCount;
            else
                _concurrencyLevel = threadCount;

            // Initialize the queue used for storing tasks
            _blockingTaskQueue = new BlockingCollection<Task>();

            // Create all of the threads
            _threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                _threads[i] = new Thread(() => ThreadBasedDispatchLoop(threadInit, threadFinally), threadMaxStackSize)
                {
                    Priority = threadPriority,
                    IsBackground = !useForegroundThreads,
                };
                if (threadName != null)
                    _threads[i].Name = threadName + " (" + i + ")";
                _threads[i].SetApartmentState(threadApartmentState);
            }

            // Start all of the threads
            foreach (var thread in _threads)
                thread.Start();
        }

        /// <summary>The dispatch loop run by all threads in this scheduler.</summary>
        /// <param name="threadInit">An initialization routine to run when the thread begins.</param>
        /// <param name="threadFinally">A finalization routine to run before the thread ends.</param>
        private void ThreadBasedDispatchLoop(Action threadInit, Action threadFinally)
        {
            s_taskProcessingThread.Value = true;
            threadInit?.Invoke();
            try
            {
                // If the scheduler is disposed, the cancellation token will be set and
                // we'll receive an OperationCanceledException.  That OCE should not crash the process.
                try
                {
                    // If a thread abort occurs, we'll try to reset it and continue running.
                    while (true)
                    {
                        try
                        {
                            // For each task queued to the scheduler, try to execute it.
                            foreach (var task in _blockingTaskQueue.GetConsumingEnumerable(_disposeCancellation.Token))
                            {
                                // If the task is not null, that means it was queued to this scheduler directly.
                                // Run it.
                                if (task != null)
                                {
                                    TryExecuteTask(task);
                                }
                                // If the task is null, that means it's just a placeholder for a task
                                // queued to one of the subschedulers.  Find the next task based on
                                // priority and fairness and run it.
                                else
                                {
                                    // Find the next task based on our ordering rules...
                                    Task targetTask;
                                    QueuedTaskSchedulerQueue queueForTargetTask;
                                    lock (_queueGroups)
                                        FindNextTask_NeedsLock(out targetTask, out queueForTargetTask);

                                    // ... and if we found one, run it
                                    if (targetTask != null)
                                        queueForTargetTask.ExecuteTask(targetTask);
                                }
                            }
                        }
                        catch (ThreadAbortException)
                        {
                            // If we received a thread abort, and that thread abort was due to shutting down
                            // or unloading, let it pass through.  Otherwise, reset the abort so we can
                            // continue processing work items.
                            if (!Environment.HasShutdownStarted && !AppDomain.CurrentDomain.IsFinalizingForUnload())
                            {
                                Thread.ResetAbort();
                            }
                        }
                    }
                }
                catch (OperationCanceledException) { }
            }
            finally
            {
                // Run a cleanup routine if there was one
                threadFinally?.Invoke();
                s_taskProcessingThread.Value = false;
            }
        }

        /// <summary>Gets the number of queues currently activated.</summary>
        private int DebugQueueCount
        {
            get
            {
                int count = 0;
                foreach (var group in _queueGroups)
                    count += group.Value.Count;
                return count;
            }
        }

        /// <summary>Gets the number of tasks currently scheduled.</summary>
        private int DebugTaskCount => (_targetScheduler != null ?
                    (IEnumerable<Task>)_nonthreadsafeTaskQueue : (IEnumerable<Task>)_blockingTaskQueue)
                    .Where(t => t != null).Count();

        /// <summary>Find the next task that should be executed, based on priorities and fairness and the like.</summary>
        /// <param name="targetTask">The found task, or null if none was found.</param>
        /// <param name="queueForTargetTask">
        /// The scheduler associated with the found task.  Due to security checks inside of TPL,  
        /// this scheduler needs to be used to execute that task.
        /// </param>
        private void FindNextTask_NeedsLock(out Task targetTask, out QueuedTaskSchedulerQueue queueForTargetTask)
        {
            targetTask = null;
            queueForTargetTask = null;

            // Look through each of our queue groups in sorted order.
            // This ordering is based on the priority of the queues.
            foreach (var queueGroup in _queueGroups)
            {
                var queues = queueGroup.Value;

                // Within each group, iterate through the queues in a round-robin
                // fashion.  Every time we iterate again and successfully find a task, 
                // we'll start in the next location in the group.
                foreach (int i in queues.CreateSearchOrder())
                {
                    queueForTargetTask = queues[i];
                    var items = queueForTargetTask._workItems;
                    if (items.Count > 0)
                    {
                        targetTask = items.Dequeue();
                        if (queueForTargetTask._disposed && items.Count == 0)
                        {
                            RemoveQueue_NeedsLock(queueForTargetTask);
                        }
                        queues.NextQueueIndex = (queues.NextQueueIndex + 1) % queueGroup.Value.Count;
                        return;
                    }
                }
            }
        }

        /// <summary>Queues a task to the scheduler.</summary>
        /// <param name="task">The task to be queued.</param>
        protected override void QueueTask(Task task)
        {
            // If we've been disposed, no one should be queueing
            if (_disposeCancellation.IsCancellationRequested)
                throw new ObjectDisposedException(GetType().Name);

            // If the target scheduler is null (meaning we're using our own threads),
            // add the task to the blocking queue
            if (_targetScheduler == null)
            {
                _blockingTaskQueue.Add(task);
            }
            // Otherwise, add the task to the non-blocking queue,
            // and if there isn't already an executing processing task,
            // start one up
            else
            {
                // Queue the task and check whether we should launch a processing
                // task (noting it if we do, so that other threads don't result
                // in queueing up too many).
                bool launchTask = false;
                lock (_nonthreadsafeTaskQueue)
                {
                    _nonthreadsafeTaskQueue.Enqueue(task);
                    if (_delegatesQueuedOrRunning < _concurrencyLevel)
                    {
                        ++_delegatesQueuedOrRunning;
                        launchTask = true;
                    }
                }

                // If necessary, start processing asynchronously
                if (launchTask)
                {
                    Task.Factory.StartNew(ProcessPrioritizedAndBatchedTasks,
                        CancellationToken.None, TaskCreationOptions.None, _targetScheduler);
                }
            }
        }

        /// <summary>
        /// Process tasks one at a time in the best order.  
        /// This should be run in a Task generated by QueueTask.
        /// It's been separated out into its own method to show up better in Parallel Tasks.
        /// </summary>
        private void ProcessPrioritizedAndBatchedTasks()
        {
            bool continueProcessing = true;
            while (!_disposeCancellation.IsCancellationRequested && continueProcessing)
            {
                try
                {
                    // Note that we're processing tasks on this thread
                    s_taskProcessingThread.Value = true;

                    // Until there are no more tasks to process
                    while (!_disposeCancellation.IsCancellationRequested)
                    {
                        // Try to get the next task.  If there aren't any more, we're done.
                        Task targetTask;
                        lock (_nonthreadsafeTaskQueue)
                        {
                            if (_nonthreadsafeTaskQueue.Count == 0)
                                break;
                            targetTask = _nonthreadsafeTaskQueue.Dequeue();
                        }

                        // If the task is null, it's a placeholder for a task in the round-robin queues.
                        // Find the next one that should be processed.
                        QueuedTaskSchedulerQueue queueForTargetTask = null;
                        if (targetTask == null)
                        {
                            lock (_queueGroups)
                                FindNextTask_NeedsLock(out targetTask, out queueForTargetTask);
                        }

                        // Now if we finally have a task, run it.  If the task
                        // was associated with one of the round-robin schedulers, we need to use it
                        // as a thunk to execute its task.
                        if (targetTask != null)
                        {
                            if (queueForTargetTask != null)
                                queueForTargetTask.ExecuteTask(targetTask);
                            else
                                TryExecuteTask(targetTask);
                        }
                    }
                }
                finally
                {
                    // Now that we think we're done, verify that there really is
                    // no more work to do.  If there's not, highlight
                    // that we're now less parallel than we were a moment ago.
                    lock (_nonthreadsafeTaskQueue)
                    {
                        if (_nonthreadsafeTaskQueue.Count == 0)
                        {
                            _delegatesQueuedOrRunning--;
                            continueProcessing = false;
                            s_taskProcessingThread.Value = false;
                        }
                    }
                }
            }
        }

        /// <summary>Notifies the pool that there's a new item to be executed in one of the round-robin queues.</summary>
        private void NotifyNewWorkItem() => QueueTask(null);

        /// <summary>Tries to execute a task synchronously on the current thread.</summary>
        /// <param name="task">The task to execute.</param>
        /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param>
        /// <returns>true if the task was executed; otherwise, false.</returns>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) =>
            // If we're already running tasks on this threads, enable inlining
            s_taskProcessingThread.Value && TryExecuteTask(task);

        /// <summary>Gets the tasks scheduled to this scheduler.</summary>
        /// <returns>An enumerable of all tasks queued to this scheduler.</returns>
        /// <remarks>This does not include the tasks on sub-schedulers.  Those will be retrieved by the debugger separately.</remarks>
        protected override IEnumerable<Task> GetScheduledTasks()
        {
            // If we're running on our own threads, get the tasks from the blocking queue...
            if (_targetScheduler == null)
            {
                // Get all of the tasks, filtering out nulls, which are just placeholders
                // for tasks in other sub-schedulers
                return _blockingTaskQueue.Where(t => t != null).ToList();
            }
            // otherwise get them from the non-blocking queue...
            else
            {
                return _nonthreadsafeTaskQueue.Where(t => t != null).ToList();
            }
        }

        /// <summary>Gets the maximum concurrency level to use when processing tasks.</summary>
        public override int MaximumConcurrencyLevel => _concurrencyLevel;

        /// <summary>Initiates shutdown of the scheduler.</summary>
        public void Dispose() => _disposeCancellation.Cancel();

        /// <summary>Creates and activates a new scheduling queue for this scheduler.</summary>
        /// <returns>The newly created and activated queue at priority 0.</returns>
        public TaskScheduler ActivateNewQueue() => ActivateNewQueue(0);

        /// <summary>Creates and activates a new scheduling queue for this scheduler.</summary>
        /// <param name="priority">The priority level for the new queue.</param>
        /// <returns>The newly created and activated queue at the specified priority.</returns>
        public TaskScheduler ActivateNewQueue(int priority)
        {
            // Create the queue
            var createdQueue = new QueuedTaskSchedulerQueue(priority, this);

            // Add the queue to the appropriate queue group based on priority
            lock (_queueGroups)
            {
                if (!_queueGroups.TryGetValue(priority, out QueueGroup list))
                {
                    list = new QueueGroup();
                    _queueGroups.Add(priority, list);
                }
                list.Add(createdQueue);
            }

            // Hand the new queue back
            return createdQueue;
        }

        /// <summary>Removes a scheduler from the group.</summary>
        /// <param name="queue">The scheduler to be removed.</param>
        private void RemoveQueue_NeedsLock(QueuedTaskSchedulerQueue queue)
        {
            // Find the group that contains the queue and the queue's index within the group
            var queueGroup = _queueGroups[queue._priority];
            int index = queueGroup.IndexOf(queue);

            // We're about to remove the queue, so adjust the index of the next
            // round-robin starting location if it'll be affected by the removal
            if (queueGroup.NextQueueIndex >= index)
                queueGroup.NextQueueIndex--;

            // Remove it
            queueGroup.RemoveAt(index);
        }

        /// <summary>A group of queues a the same priority level.</summary>
        private class QueueGroup : List<QueuedTaskSchedulerQueue>
        {
            /// <summary>The starting index for the next round-robin traversal.</summary>
            public int NextQueueIndex = 0;

            /// <summary>Creates a search order through this group.</summary>
            /// <returns>An enumerable of indices for this group.</returns>
            public IEnumerable<int> CreateSearchOrder()
            {
                for (int i = NextQueueIndex; i < Count; i++)
                    yield return i;
                for (int i = 0; i < NextQueueIndex; i++)
                    yield return i;
            }
        }

        /// <summary>Provides a scheduling queue associatd with a QueuedTaskScheduler.</summary>
        [DebuggerDisplay("QueuePriority = {_priority}, WaitingTasks = {WaitingTasks}")]
        [DebuggerTypeProxy(typeof(QueuedTaskSchedulerQueueDebugView))]
        private sealed class QueuedTaskSchedulerQueue : TaskScheduler, IDisposable
        {
            /// <summary>A debug view for the queue.</summary>
            private sealed class QueuedTaskSchedulerQueueDebugView
            {
                /// <summary>The queue.</summary>
                private readonly QueuedTaskSchedulerQueue _queue;

                /// <summary>Initializes the debug view.</summary>
                /// <param name="queue">The queue to be debugged.</param>
                public QueuedTaskSchedulerQueueDebugView(QueuedTaskSchedulerQueue queue) =>
                    _queue = queue ?? throw new ArgumentNullException(nameof(queue));

                /// <summary>Gets the priority of this queue in its associated scheduler.</summary>
                public int Priority => _queue._priority;
                /// <summary>Gets the ID of this scheduler.</summary>
                public int Id => _queue.Id;
                /// <summary>Gets all of the tasks scheduled to this queue.</summary>
                public IEnumerable<Task> ScheduledTasks => _queue.GetScheduledTasks();
                /// <summary>Gets the QueuedTaskScheduler with which this queue is associated.</summary>
                public QueuedTaskScheduler AssociatedScheduler => _queue._pool;
            }

            /// <summary>The scheduler with which this pool is associated.</summary>
            private readonly QueuedTaskScheduler _pool;
            /// <summary>The work items stored in this queue.</summary>
            internal readonly Queue<Task> _workItems;
            /// <summary>Whether this queue has been disposed.</summary>
            internal bool _disposed;
            /// <summary>Gets the priority for this queue.</summary>
            internal int _priority;

            /// <summary>Initializes the queue.</summary>
            /// <param name="priority">The priority associated with this queue.</param>
            /// <param name="pool">The scheduler with which this queue is associated.</param>
            internal QueuedTaskSchedulerQueue(int priority, QueuedTaskScheduler pool)
            {
                _priority = priority;
                _pool = pool;
                _workItems = new Queue<Task>();
            }

            /// <summary>Gets the number of tasks waiting in this scheduler.</summary>
            internal int WaitingTasks => _workItems.Count;

            /// <summary>Gets the tasks scheduled to this scheduler.</summary>
            /// <returns>An enumerable of all tasks queued to this scheduler.</returns>
            protected override IEnumerable<Task> GetScheduledTasks() => _workItems.ToList();

            /// <summary>Queues a task to the scheduler.</summary>
            /// <param name="task">The task to be queued.</param>
            protected override void QueueTask(Task task)
            {
                if (_disposed)
                    throw new ObjectDisposedException(GetType().Name);

                // Queue up the task locally to this queue, and then notify
                // the parent scheduler that there's work available
                lock (_pool._queueGroups)
                    _workItems.Enqueue(task);
                _pool.NotifyNewWorkItem();
            }

            /// <summary>Tries to execute a task synchronously on the current thread.</summary>
            /// <param name="task">The task to execute.</param>
            /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param>
            /// <returns>true if the task was executed; otherwise, false.</returns>
            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) =>
                // If we're using our own threads and if this is being called from one of them,
                // or if we're currently processing another task on this thread, try running it inline.
                s_taskProcessingThread.Value && TryExecuteTask(task);

            /// <summary>Runs the specified ask.</summary>
            /// <param name="task">The task to execute.</param>
            internal void ExecuteTask(Task task) => TryExecuteTask(task);

            /// <summary>Gets the maximum concurrency level to use when processing tasks.</summary>
            public override int MaximumConcurrencyLevel => _pool.MaximumConcurrencyLevel;

            /// <summary>Signals that the queue should be removed from the scheduler as soon as the queue is empty.</summary>
            public void Dispose()
            {
                if (!_disposed)
                {
                    lock (_pool._queueGroups)
                    {
                        // We only remove the queue if it's empty.  If it's not empty,
                        // we still mark it as disposed, and the associated QueuedTaskScheduler
                        // will remove the queue when its count hits 0 and its _disposed is true.
                        if (_workItems.Count == 0)
                        {
                            _pool.RemoveQueue_NeedsLock(this);
                        }
                    }
                    _disposed = true;
                }
            }
        }
    }
}
