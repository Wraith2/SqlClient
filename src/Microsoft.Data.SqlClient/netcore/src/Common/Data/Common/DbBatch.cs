// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace System.Data.Common
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
    public abstract partial class DbBatch : IDisposable
    {
        protected class DbBatchCommandList<TCommand> : IList<DbBatchCommand> where TCommand : DbBatchCommand
        {
            private readonly List<TCommand> _batchCommands;

            public DbBatchCommandList(List<TCommand> batchCommands) => _batchCommands = batchCommands;

            public void Add(DbBatchCommand item) => _batchCommands.Add((TCommand)item);

            public void Clear() => _batchCommands.Clear();

            public bool Contains(DbBatchCommand item) => _batchCommands.Contains((TCommand)item);

            public IEnumerator<DbBatchCommand> GetEnumerator() => _batchCommands.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public bool Remove(DbBatchCommand item) => _batchCommands.Remove((TCommand)item);

            public int Count => _batchCommands.Count;

            public bool IsReadOnly => false;

            public int IndexOf(DbBatchCommand item) => _batchCommands.IndexOf((TCommand)item);

            public void Insert(int index, DbBatchCommand item) => _batchCommands.Insert(index, (TCommand)item);

            public void RemoveAt(int index) => _batchCommands.RemoveAt(index);

            public DbBatchCommand this[int index]
            {
                get => _batchCommands[index];
                set => _batchCommands[index] = (TCommand)value;
            }

            public void CopyTo(DbBatchCommand[] array, int arrayIndex)
            {
                for (var i = 0; i < _batchCommands.Count; i++)
                {
                    array[arrayIndex + i] = _batchCommands[i];
                }
            }
        }

        public IList<DbBatchCommand> BatchCommands => DbBatchCommands;
        protected abstract IList<DbBatchCommand> DbBatchCommands { get; }

        public DbDataReader ExecuteDbReader() => ExecuteDbDataReader();
        protected abstract DbDataReader ExecuteDbDataReader();

        public Task<DbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken = default) => ExecuteDbDataReaderAsync(cancellationToken);
        protected abstract Task<DbDataReader> ExecuteDbDataReaderAsync(CancellationToken cancellationToken);

        public abstract int ExecuteNonQuery();
        public abstract Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default);

        public abstract object ExecuteScalar();
        public abstract Task<object> ExecuteScalarAsync(CancellationToken cancellationToken = default);

        public abstract int Timeout { get; set; }

        public DbConnection Connection { get => DbConnection; set => DbConnection = value; }

        protected abstract DbConnection DbConnection { get; set; }

        // Delegates to DbTransaction
        public DbTransaction Transaction { get => DbTransaction; set => DbTransaction = value; }
        protected abstract DbTransaction DbTransaction { get; set; }


        public abstract void Prepare();
        public abstract Task PrepareAsync(CancellationToken cancellationToken = default);
        public abstract void Cancel();

        public void Dispose() => Dispose(true);

        protected virtual void Dispose(bool disposing) { }


    }
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}
