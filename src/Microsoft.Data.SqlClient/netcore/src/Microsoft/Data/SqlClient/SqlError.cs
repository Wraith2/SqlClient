// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Data.SqlClient
{
    /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient/SqlError.xml' path='docs/members[@name="SqlError"]/SqlError/*' />
    public sealed class SqlError
    {
        private string _source = TdsEnums.SQL_PROVIDER_NAME;
        private int _number;
        private byte _state;
        private byte _errorClass;
        private string _server;
        private string _message;
        private string _procedure;
        private int _lineNumber;
        private int _win32ErrorCode;
        private Exception _exception;
        private int _batchIndex;

        internal SqlError(int infoNumber, byte errorState, byte errorClass, string server, string errorMessage, string procedure, int lineNumber, uint win32ErrorCode, Exception exception = null, int batchIndex = -1)
            : this(infoNumber, errorState, errorClass, server, errorMessage, procedure, lineNumber, exception, batchIndex)
        {
            _win32ErrorCode = (int)win32ErrorCode;
        }

        internal SqlError(int infoNumber, byte errorState, byte errorClass, string server, string errorMessage, string procedure, int lineNumber, Exception exception = null, int batchIndex = -1)
        {
            _number = infoNumber;
            _state = errorState;
            _errorClass = errorClass;
            _server = server;
            _message = errorMessage;
            _procedure = procedure;
            _lineNumber = lineNumber;
            _win32ErrorCode = 0;
            _exception = exception;
            _batchIndex = batchIndex;
            if (errorClass != 0)
            {
                SqlClientEventSource.Log.TraceEvent("<sc.SqlError.SqlError|ERR> infoNumber={0}, errorState={1}, errorClass={2}, errorMessage='{3}', procedure='{4}', lineNumber={5}", infoNumber, (int)errorState, (int)errorClass, errorMessage, procedure ?? "None", (int)lineNumber);
            }
        }

        /// <include file='../../../../../../../doc/snippets/Microsoft.Data.SqlClient/SqlError.xml' path='docs/members[@name="SqlError"]/ToString/*' />
        // There is no exception stack included because the correct exception stack is only available 
        // on SqlException, and to obtain that the SqlError would have to have backpointers all the
        // way back to SqlException.  If the user needs a call stack, they can obtain it on SqlException.
        public override string ToString()
        {
            return typeof(SqlError).ToString() + ": " + _message; // since this is sealed so we can change GetType to typeof
        }

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/Source/*' />
        public string Source => _source;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/Number/*' />
        public int Number => _number;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/State/*' />
        public byte State => _state;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/Class/*' />
        public byte Class => _errorClass;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/Server/*' />
        public string Server => _server;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/Message/*' />
        public string Message => _message;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/Procedure/*' />
        public string Procedure => _procedure;

        /// <include file='..\..\..\..\..\..\..\doc\snippets\Microsoft.Data.SqlClient\SqlError.xml' path='docs/members[@name="SqlError"]/LineNumber/*' />
        public int LineNumber => _lineNumber;

        internal int Win32ErrorCode => _win32ErrorCode;

        internal Exception Exception => _exception;

        internal int BatchIndex => _batchIndex;
    }
}
