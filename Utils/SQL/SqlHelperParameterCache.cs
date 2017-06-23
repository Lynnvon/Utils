using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;

namespace Utils.SQL
{
    internal static class SqlHelperParameterCache
    {
        private static readonly Hashtable parameterCache = Hashtable.Synchronized(new Hashtable());

        private static DbParameter[] DiscoverSpParameterSet(OleDbConnection connection, string storedProcedureName, bool includeReturnValueParameter)
        {
            if (connection == null)
                throw new ArgumentNullException("Connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("StoredProcedureName");
            OleDbCommand command = new OleDbCommand(storedProcedureName, connection);
            command.CommandType = CommandType.StoredProcedure;
            connection.Open();
            OleDbCommandBuilder.DeriveParameters(command);
            connection.Close();
            if (!includeReturnValueParameter)
                command.Parameters.RemoveAt(0);
            DbParameter[] array = new DbParameter[command.Parameters.Count];
            command.Parameters.CopyTo(array, 0);
            foreach (DbParameter dbParameter in array)
                dbParameter.Value = (object)DBNull.Value;
            return array;
        }

        private static DbParameter[] CloneParameters(DbParameter[] originalParameters)
        {
            DbParameter[] sqlParameterArray = new DbParameter[originalParameters.Length];
            int index = 0;
            for (int length = originalParameters.Length; index < length; ++index)
                sqlParameterArray[index] = (DbParameter)((ICloneable)originalParameters[index]).Clone();
            return sqlParameterArray;
        }

        public static void CacheParameterSet(string connectionString, string commandText, params DbParameter[] sqlParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("ConnectionString");
            if (string.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("CommandText");
            string str = connectionString + ":" + commandText;
            SqlHelperParameterCache.parameterCache[(object)str] = (object)sqlParameters;
        }

        public static DbParameter[] GetCachedParameterSet(string connectionString, string commandText)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("ConnectionString");
            if (string.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("CommandText");
            string str = connectionString + ":" + commandText;
            DbParameter[] originalParameters = SqlHelperParameterCache.parameterCache[(object)str] as DbParameter[];
            if (originalParameters == null)
                return (DbParameter[])null;
            return SqlHelperParameterCache.CloneParameters(originalParameters);
        }

        public static DbParameter[] GetSpParameterSet(string connectionString, string storedProcedureName)
        {
            return SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName, false);
        }

        public static DbParameter[] GetSpParameterSet(string connectionString, string storedProcedureName, bool includeReturnValueParameter)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("ConnectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("StoredProcedureName");
            using (OleDbConnection connection = new OleDbConnection(connectionString))
                return SqlHelperParameterCache.GetSpParameterSetInternal(connection, storedProcedureName, includeReturnValueParameter);
        }

        internal static DbParameter[] GetSpParameterSet(DbConnection connection, string storedProcedureName)
        {
            return SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName, false);
        }

        internal static DbParameter[] GetSpParameterSet(DbConnection connection, string storedProcedureName, bool includeReturnValueParameter)
        {
            if (connection == null)
                throw new ArgumentNullException("Connection");
            using (OleDbConnection connection1 = (OleDbConnection)((ICloneable)connection).Clone())
                return SqlHelperParameterCache.GetSpParameterSetInternal(connection1, storedProcedureName, includeReturnValueParameter);
        }

        private static DbParameter[] GetSpParameterSetInternal(OleDbConnection connection, string storedProcedureName, bool includeReturnValueParameter)
        {
            if (connection == null)
                throw new ArgumentNullException("Connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("StoredProcedureName");
            string str = connection.ConnectionString + ":" + storedProcedureName + (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");
            DbParameter[] originalParameters = SqlHelperParameterCache.parameterCache[(object)str] as DbParameter[];
            if (originalParameters == null)
            {
                DbParameter[] sqlParameterArray = SqlHelperParameterCache.DiscoverSpParameterSet(connection, storedProcedureName, includeReturnValueParameter);
                SqlHelperParameterCache.parameterCache[(object)str] = (object)sqlParameterArray;
                originalParameters = sqlParameterArray;
            }
            return SqlHelperParameterCache.CloneParameters(originalParameters);
        }
    }
}
