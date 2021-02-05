// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Microsoft.Data.SqlClient.SNI
{
    /// <summary>
    /// SNI MARS connection. Multiple MARS streams will be overlaid on this connection.
    /// </summary>
    internal class SNIMarsConnection
    {
        private readonly Guid _connectionId;
        private readonly Dictionary<int, SNIMarsHandle> _sessions;
        private SNIHandle _lowerHandle;
        private ushort _nextSessionId;

        /// <summary>
        /// Connection ID
        /// </summary>
        public Guid ConnectionId => _connectionId;

        public int ProtocolVersion => _lowerHandle.ProtocolVersion;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="lowerHandle">Lower handle</param>
        public SNIMarsConnection(SNIHandle lowerHandle)
        {
            _connectionId = Guid.NewGuid();
            _sessions = new Dictionary<int, SNIMarsHandle>();
            _state = State.Header;
            _headerCount = 0;
            _headerBytes = new byte[SNISMUXHeader.HEADER_LENGTH];
            _header = new SNISMUXHeader();
            _nextSessionId = 0;
            _lowerHandle = lowerHandle;
            _lowerHandle.SetAsyncCallbacks(HandleReceiveComplete, HandleSendComplete);
        }

        public SNIMarsHandle CreateMarsSession(object callbackObject, bool async)
        {
            lock (this)
            {
                ushort sessionId = _nextSessionId++;
                SNIMarsHandle handle = new SNIMarsHandle(this, sessionId, callbackObject, async);
                _sessions.Add(sessionId, handle);
                return handle;
            }
        }

        /// <summary>
        /// Start receiving
        /// </summary>
        /// <returns></returns>
        public uint StartReceive()
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.StartReceive |SNI|INFO|SCOPE> StartReceive");
            try
            {
                SNIPacket packet = null;

                if (ReceiveAsync(ref packet) == TdsEnums.SNI_SUCCESS_IO_PENDING)
                {
                    SqlClientEventSource.Log.TrySNITraceEvent("<sc.SNI.SNIMarsConnection.StartReceive |SNI|INFO|Trace> Success IO pending.");
                    return TdsEnums.SNI_SUCCESS_IO_PENDING;
                }
                SqlClientEventSource.Log.TrySNITraceEvent("<sc.SNI.SNIMarsConnection.StartReceive |SNI|ERR> Connection not useable.");
                return SNICommon.ReportSNIError(SNIProviders.SMUX_PROV, 0, SNICommon.ConnNotUsableError, string.Empty);
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }

        /// <summary>
        /// Send a packet synchronously
        /// </summary>
        /// <param name="packet">SNI packet</param>
        /// <returns>SNI error code</returns>
        public uint Send(SNIPacket packet)
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.Send |SNI|INFO|SCOPE> Send");
            try
            {
                lock (this)
                {
                    return _lowerHandle.Send(packet);
                }
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
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
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.SendAsync |SNI|INFO|SCOPE> SendAsync");
            try
            {
                lock (this)
                {
                    return _lowerHandle.SendAsync(packet, callback);
                }
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }

        /// <summary>
        /// Receive a packet asynchronously
        /// </summary>
        /// <param name="packet">SNI packet</param>
        /// <returns>SNI error code</returns>
        public uint ReceiveAsync(ref SNIPacket packet)
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.SendAsync |SNI|INFO|SCOPE> SendAsync");
            try
            {
                if (packet != null)
                {
                    ReturnPacket(packet);
                    packet = null;
                }

                lock (this)
                {
                    return _lowerHandle.ReceiveAsync(ref packet);
                }
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }

        /// <summary>
        /// Check SNI handle connection
        /// </summary>
        /// <returns>SNI error status</returns>
        public uint CheckConnection()
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.CheckConnection |SNI|INFO|SCOPE>");
            try
            {
                lock (this)
                {
                    return _lowerHandle.CheckConnection();
                }
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }

        /// <summary>
        /// Process a receive error
        /// </summary>
        public void HandleReceiveError(SNIPacket packet)
        {
            Debug.Assert(Monitor.IsEntered(this), "HandleReceiveError was called without being locked.");
            if (!Monitor.IsEntered(this))
            {
                SqlClientEventSource.Log.TrySNITraceEvent("<sc.SNI.SNIMarsConnection.HandleReceiveError |SNI|ERR> HandleReceiveError was called without being locked.");
            }
            foreach (SNIMarsHandle handle in _sessions.Values)
            {
                if (packet.HasCompletionCallback)
                {
                    handle.HandleReceiveError(packet);
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
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.EnableSsl |SNI|INFO|SCOPE>");
            try
            {
                return _lowerHandle.EnableSsl(options);
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }

        /// <summary>
        /// Disable SSL
        /// </summary>
        public void DisableSsl()
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.EnableSsl |SNI|INFO|SCOPE>");
            try
            {
                _lowerHandle.DisableSsl();
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }

        public SNIPacket RentPacket(int headerSize, int dataSize)
        {
            return _lowerHandle.RentPacket(headerSize, dataSize);
        }

        public void ReturnPacket(SNIPacket packet)
        {
            SNIHandle handle = _lowerHandle;
            if (handle != null)
            {
                handle.ReturnPacket(packet);
            }
            else
            {
                packet.Release();
            }
        }

#if DEBUG
        /// <summary>
        /// Test handle for killing underlying connection
        /// </summary>
        public void KillConnection()
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.KillConnection |SNI|INFO|SCOPE>");
            try
            {
                _lowerHandle.KillConnection();
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }
#endif

        private enum State : uint
        {
            Header = 1,
            Payload = 2,
            Dispatch = 3
        }

        private enum LoopState : uint
        {
            Run,
            Recieve,
            Finish,
            Error
        }


        // the following variables are used only inside HandleRecieveComplete
        // all access to these variables must be performed under lock(this) because
        // RecieveAsync can immediately return a new packet causing overlapping
        // behaviour without the lock.
        private State _state;

        private byte[] _headerBytes;
        private int _headerCount;
        private SNISMUXHeader _header;

        private int _payloadLength;
        private int _payloadCount;
        private SNIPacket _partial;

        public void HandleReceiveComplete(SNIPacket packet, uint sniErrorCode)
        {
            long scopeID = SqlClientEventSource.Log.TrySNIScopeEnterEvent("<sc.SNI.SNIMarsConnection.HandleReceiveComplete |SNI|INFO|SCOPE>");
            try
            {
                if (sniErrorCode != TdsEnums.SNI_SUCCESS)
                {
                    lock (this)
                    {
                        HandleReceiveError(packet);
                        SqlClientEventSource.Log.TrySNITraceEvent("<sc.SNI.SNIMarsConnection.HandleReceiveComplete |SNI|ERR> not successful.");
                        return;
                    }
                }

                LoopState loopState = LoopState.Run;
                lock (this)
                {
                    while (loopState == LoopState.Run)
                    {
                        switch (_state)
                        {
                            case State.Header:
                                int taken = packet.TakeData(_headerBytes, _headerCount, SNISMUXHeader.HEADER_LENGTH - _headerCount);
                                _headerCount += taken;
                                if (_headerCount == SNISMUXHeader.HEADER_LENGTH)
                                {
                                    _header.Read(_headerBytes);
                                    _payloadLength = (int)_header.length;
                                    _payloadCount = 0;
                                    _partial = RentPacket(headerSize: 0, dataSize: _payloadLength);
                                    _state = State.Payload;
                                    goto case State.Payload;
                                }
                                else
                                {
                                    loopState = LoopState.Recieve;
                                }
                                break;

                            case State.Payload:
                                if (packet.DataLeft == _payloadLength && _partial == null)
                                {
                                    // if the data in the packet being processed is exactly and only the data that is going to sent
                                    // on to the parser then don't copy it to a new packet just forward the current packet once we've
                                    // fiddled the data pointer so that it skips the header data when
                                    _partial = packet;
                                    packet = null;
                                    _partial.SetDataToRemainingContents();
                                    _state = State.Dispatch;
                                    goto case State.Dispatch;
                                }
                                else
                                {
                                    int wanted = _payloadLength - _payloadCount;
                                    int transferred = SNIPacket.TransferData(packet, _partial, wanted);
                                    _payloadCount += transferred;
                                    if (_payloadCount == _payloadLength)
                                    {
                                        // payload is complete so dispatch the current packet
                                        _state = State.Dispatch;
                                        goto case State.Dispatch;
                                    }
                                    else if (packet.DataLeft == 0)
                                    {
                                        // no more data in this packet so wait for a new one
                                        loopState = LoopState.Recieve;
                                    }
                                    else
                                    {
                                        // start the loop again and decode the next packet in the input
                                        _headerCount = 0;
                                        _state = State.Header;
                                    }
                                }

                                break;

                            case State.Dispatch:
                                if (_sessions.TryGetValue(_header.sessionId, out SNIMarsHandle session))
                                {
                                    switch ((SNISMUXFlags)_header.flags)
                                    {
                                        case SNISMUXFlags.SMUX_DATA:
                                            session.HandleReceiveComplete(_partial, _header);
                                            // do not return the _partial packet, the receiver is responsible for returning the 
                                            // packet once it has been used because it can take sync and async paths from here
                                            _partial = null;
                                            break;

                                        case SNISMUXFlags.SMUX_ACK:
                                            ReturnPacket(_partial);
                                            _partial = null;
                                            try
                                            {
                                                session.HandleAck(_header.highwater);
                                            }
                                            catch (Exception e)
                                            {
                                                SNICommon.ReportSNIError(SNIProviders.SMUX_PROV, SNICommon.InternalExceptionError, e);
                                            }
                                            break;

                                        case SNISMUXFlags.SMUX_FIN:
                                            ReturnPacket(_partial);
                                            _partial = null;
                                            _sessions.Remove(_header.sessionId);
                                            break;

                                        default:
                                            Debug.Fail("unknown smux packet flag");
                                            break;
                                    }

                                    // a packet has been dispatched so change to header state eeady to decode another
                                    _headerCount = 0;
                                    _state = State.Header;

                                    if (packet == null || packet.DataLeft == 0)
                                    {
                                        // no more data in this packet or the packet has been forwarded so exit 
                                        // the loop and wait for a new packet to be recieved
                                        loopState = LoopState.Recieve;
                                    }
                                }
                                else
                                {
                                    SNILoadHandle.SingletonInstance.LastError = new SNIError(SNIProviders.SMUX_PROV, 0, SNICommon.InvalidParameterError, string.Empty);
                                    HandleReceiveError(packet);
                                    packet = null;
                                    _lowerHandle.Dispose();
                                    _lowerHandle = null;
                                    loopState = LoopState.Error;
                                }
                                break;
                        }
                    }
                }

                if (loopState == LoopState.Recieve)
                {
                    if (packet != null)
                    {
                        Debug.Assert(packet.DataLeft == 0, "loop exit with data remaining");
                        ReturnPacket(packet);
                        packet = null;
                    }

                    if (ReceiveAsync(ref packet) == TdsEnums.SNI_SUCCESS_IO_PENDING)
                    {
                        SqlClientEventSource.Log.TrySNITraceEvent("<sc.SNI.SNIMarsConnection.HandleReceiveComplete |SNI|ERR> not successful.");
                        packet = null;
                    }
                    else
                    {
                        HandleReceiveError(packet);
                    }
                }
            }
            finally
            {
                SqlClientEventSource.Log.TrySNIScopeLeaveEvent(scopeID);
            }
        }
    }
}
