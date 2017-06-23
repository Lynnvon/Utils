using System;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Xml;

namespace Utils.SQL
{
    public static class SqlHelper
    {
        private static void AttachParameters(DbCommand command, DbParameter[] DbParameters)
        {
            if (command == null)
                throw new ArgumentNullException("command");
            foreach (DbParameter DbParameter in DbParameters)
            {
                if (DbParameter != null)
                {
                    if ((DbParameter.Direction == ParameterDirection.Input || DbParameter.Direction == ParameterDirection.InputOutput) && DbParameter.Value == null)
                        DbParameter.Value = (object)DBNull.Value;
                    command.Parameters.Add(DbParameter);
                }
            }
        }

        private static void AssignParameterValues(DbParameter[] DbParameters, DataRow dataRow)
        {
            if (DbParameters == null || dataRow == null)
                return;
            int num = 0;
            foreach (DbParameter DbParameter in DbParameters)
            {
                if (DbParameter.ParameterName == null || DbParameter.ParameterName.Length <= 1)
                    throw new Exception(string.Format("Please provide a valid parameter name on the parameter #{0}, the ParameterName property has the following value: '{1}'.", (object)num, (object)DbParameter.ParameterName));
                if (dataRow.Table.Columns.IndexOf(DbParameter.ParameterName.Substring(1)) != -1)
                    DbParameter.Value = dataRow[DbParameter.ParameterName.Substring(1)];
                ++num;
            }
        }

        private static void AssignParameterValues(DbParameter[] DbParameters, object[] parameterValues)
        {
            if (DbParameters == null || parameterValues == null)
                return;
            if (DbParameters.Length != parameterValues.Length)
                throw new ArgumentException("Parameter count does not match Parameter Value count.");
            int index = 0;
            for (int length = DbParameters.Length; index < length; ++index)
            {
                if (parameterValues[index] is IDbDataParameter)
                {
                    IDbDataParameter parameterValue = (IDbDataParameter)parameterValues[index];
                    if (parameterValue.Value == null)
                        DbParameters[index].Value = (object)DBNull.Value;
                    else
                        DbParameters[index].Value = parameterValue.Value;
                }
                else if (parameterValues[index] == null)
                    DbParameters[index].Value = (object)DBNull.Value;
                else
                    DbParameters[index].Value = parameterValues[index];
            }
        }

        private static void PrepareCommand(DbCommand command, DbConnection connection, DbTransaction transaction, CommandType commandType, string commandText, DbParameter[] DbParameters, out bool mustCloseConnection)
        {
            if (command == null)
                throw new ArgumentNullException("command");
            if (command == null || commandText.Length == 0)
                throw new ArgumentNullException("commandText");
            if (connection.State != ConnectionState.Open)
            {
                mustCloseConnection = true;
                connection.Open();
            }
            else
                mustCloseConnection = false;
            command.Connection = connection;
            command.CommandText = commandText;
            if (transaction != null)
            {
                if (transaction.Connection == null)
                    throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
                command.Transaction = transaction;
            }
            command.CommandType = commandType;
            if (DbParameters == null)
                return;
            SqlHelper.AttachParameters(command, DbParameters);
        }

        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteNonQuery(connectionString, commandType, commandText, (DbParameter)null);
        }

        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            using (DbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                return SqlHelper.ExecuteNonQuery(connection, commandType, commandText, DbParameters);
            }
        }

        public static int ExecuteNonQuery(DbConnection connection, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            DbCommand command = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(command, connection, (DbTransaction)null, commandType, commandText, DbParameters, out mustCloseConnection);
            int num = command.ExecuteNonQuery();
            command.Parameters.Clear();
            if (mustCloseConnection)
                connection.Close();
            return num;
        }

        public static int ExecuteNonQuery(DbConnection connection, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteNonQuery(connection, commandType, commandText, (DbParameter)null);
        }

        public static int ExecuteNonQuery(string connectionString, string storedProcedureName, params object[] parameterValues)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static int ExecuteNonQuery(DbConnection connection, string storedProcedureName, params object[] parameterValues)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static int ExecuteNonQuery(DbTransaction transaction, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            DbCommand command = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(command, transaction.Connection, transaction, commandType, commandText, DbParameters, out mustCloseConnection);
            int num = command.ExecuteNonQuery();
            command.Parameters.Clear();
            return num;
        }

        public static int ExecuteNonQuery(DbTransaction transaction, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteNonQuery(transaction, commandType, commandText, (DbParameter)null);
        }

        public static int ExecuteNonQuery(DbTransaction transaction, string storedProcedureName, params object[] parameterValues)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DataSet ExecuteDataSet(string connectionString, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteDataSet(connectionString, commandType, commandText, (DbParameter[])null);
        }

        public static DataSet ExecuteDataSet(string connectionString, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            using (DbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                return SqlHelper.ExecuteDataSet(connection, commandType, commandText, DbParameters);
            }
        }

        public static DataSet ExecuteDataSet(string connectionString, string storedProcedureName, params object[] parameterValues)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteDataSet(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteDataSet(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DataSet ExecuteDataSet(DbConnection connection, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteDataSet(connection, commandType, commandText, (DbParameter)null);
        }

        public static DataSet ExecuteDataSet(DbConnection connection, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            OleDbCommand OleDbCommand = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(OleDbCommand, connection, (DbTransaction)null, commandType, commandText, DbParameters, out mustCloseConnection);
            using (DbDataAdapter OleDbDataAdapter = new OleDbDataAdapter(OleDbCommand))
            {
                DataSet dataSet = new DataSet();
                OleDbDataAdapter.Fill(dataSet);
                OleDbCommand.Parameters.Clear();
                if (mustCloseConnection)
                    connection.Close();
                return dataSet;
            }
        }

        public static DataSet ExecuteDataSet(DbConnection connection, string storedProcedureName, params object[] parameterValues)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteDataSet(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteDataSet(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DataSet ExecuteDataSet(DbTransaction transaction, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteDataSet(transaction, commandType, commandText, (DbParameter)null);
        }

        public static DataSet ExecuteDataSet(DbTransaction transaction, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            OleDbCommand OleDbCommand = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(OleDbCommand, transaction.Connection, transaction, commandType, commandText, DbParameters, out mustCloseConnection);
            using (OleDbDataAdapter OleDbDataAdapter = new OleDbDataAdapter(OleDbCommand))
            {
                DataSet dataSet = new DataSet();
                OleDbDataAdapter.Fill(dataSet);
                OleDbCommand.Parameters.Clear();
                return dataSet;
            }
        }

        public static DataSet ExecuteDataSet(DbTransaction transaction, string storedProcedureName, params object[] parameterValues)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteDataSet(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteDataSet(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        private static OleDbDataReader ExecuteReader(DbConnection connection, DbTransaction transaction, CommandType commandType, string commandText, DbParameter[] DbParameters, SqlConnectionOwnership connectionOwnership)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            bool mustCloseConnection = false;
            OleDbCommand command = new OleDbCommand();
            try
            {
                SqlHelper.PrepareCommand(command, connection, transaction, commandType, commandText, DbParameters, out mustCloseConnection);
                OleDbDataReader sqlDataReader = connectionOwnership != SqlConnectionOwnership.External ? command.ExecuteReader(CommandBehavior.CloseConnection) : command.ExecuteReader();
                bool flag = true;
                foreach (DbParameter parameter in (DbParameterCollection)command.Parameters)
                {
                    if (parameter.Direction != ParameterDirection.Input)
                        flag = false;
                }
                if (flag)
                    command.Parameters.Clear();
                return sqlDataReader;
            }
            catch
            {
                if (mustCloseConnection)
                    connection.Close();
                throw;
            }
        }

        public static DbDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteReader(connectionString, commandType, commandText, (DbParameter[])null);
        }

        public static DbDataReader ExecuteReader(string connectionString, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            DbConnection connection = (DbConnection)null;
            try
            {
                connection = new OleDbConnection(connectionString);
                connection.Open();
                return SqlHelper.ExecuteReader(connection, (DbTransaction)null, commandType, commandText, DbParameters, SqlConnectionOwnership.Internal);
            }
            catch
            {
                if (connection != null)
                    connection.Close();
                throw;
            }
        }

        public static DbDataReader ExecuteReader(string connectionString, string storedProcedureName, params object[] parameterValues)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DbDataReader ExecuteReader(DbConnection connection, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteReader(connection, commandType, commandText, (DbParameter)null);
        }

        public static DbDataReader ExecuteReader(DbConnection connection, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            return SqlHelper.ExecuteReader(connection, (DbTransaction)null, commandType, commandText, DbParameters, SqlConnectionOwnership.External);
        }

        public static DbDataReader ExecuteReader(DbConnection connection, string storedProcedureName, params object[] parameterValues)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteReader(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteReader(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DbDataReader ExecuteReader(DbTransaction transaction, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteReader(transaction, commandType, commandText, (DbParameter)null);
        }

        public static DbDataReader ExecuteReader(DbTransaction transaction, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            return SqlHelper.ExecuteReader(transaction.Connection, transaction, commandType, commandText, DbParameters, SqlConnectionOwnership.External);
        }

        public static DbDataReader ExecuteReader(DbTransaction transaction, string storedProcedureName, params object[] parameterValues)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteReader(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteReader(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteScalar(connectionString, commandType, commandText, (DbParameter[])null);
        }

        public static object ExecuteScalar(string connectionString, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            using (DbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                return SqlHelper.ExecuteScalar(connection, commandType, commandText, DbParameters);
            }
        }

        public static object ExecuteScalar(string connectionString, string storedProcedureName, params object[] parameterValues)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static object ExecuteScalar(DbConnection connection, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteScalar(connection, commandType, commandText, (DbParameter)null);
        }

        public static object ExecuteScalar(DbConnection connection, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            DbCommand command = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(command, connection, (DbTransaction)null, commandType, commandText, DbParameters, out mustCloseConnection);
            object obj = command.ExecuteScalar();
            command.Parameters.Clear();
            if (mustCloseConnection)
                connection.Close();
            return obj;
        }

        public static object ExecuteScalar(DbConnection connection, string storedProcedureName, params object[] parameterValues)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteScalar(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteScalar(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static object ExecuteScalar(DbTransaction transaction, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteScalar(transaction, commandType, commandText, (DbParameter)null);
        }

        public static object ExecuteScalar(DbTransaction transaction, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            DbCommand command = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(command, transaction.Connection, transaction, commandType, commandText, DbParameters, out mustCloseConnection);
            object obj = command.ExecuteScalar();
            command.Parameters.Clear();
            return obj;
        }

        public static object ExecuteScalar(DbTransaction transaction, string storedProcedureName, params object[] parameterValues)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static XmlReader ExecuteXmlReader(DbConnection connection, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteXmlReader(connection, commandType, commandText, (DbParameter[])null);
        }

        public static XmlReader ExecuteXmlReader(DbConnection connection, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            bool mustCloseConnection = false;
            SqlCommand command = new SqlCommand();
            try
            {
                SqlHelper.PrepareCommand(command, connection, (DbTransaction)null, commandType, commandText, DbParameters, out mustCloseConnection);
                XmlReader xmlReader = command.ExecuteXmlReader();
                command.Parameters.Clear();
                return xmlReader;
            }
            catch
            {
                if (mustCloseConnection)
                    connection.Close();
                throw;
            }
        }

        public static XmlReader ExecuteXmlReader(DbConnection connection, string storedProcedureName, params object[] parameterValues)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static XmlReader ExecuteXmlReader(DbTransaction transaction, CommandType commandType, string commandText)
        {
            return SqlHelper.ExecuteXmlReader(transaction, commandType, commandText, (DbParameter[])null);
        }

        public static XmlReader ExecuteXmlReader(DbTransaction transaction, CommandType commandType, string commandText, params DbParameter[] DbParameters)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            SqlCommand command = new SqlCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(command, transaction.Connection, transaction, commandType, commandText, DbParameters, out mustCloseConnection);
            XmlReader xmlReader = command.ExecuteXmlReader();
            command.Parameters.Clear();
            return xmlReader;
        }

        public static XmlReader ExecuteXmlReader(DbTransaction transaction, string storedProcedureName, params object[] parameterValues)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues == null || parameterValues.Length <= 0)
                return SqlHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
            return SqlHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static void FillDataSet(string connectionString, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (dataSet == null)
                throw new ArgumentNullException("dataSet");
            using (DbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                SqlHelper.FillDataSet(connection, commandType, commandText, dataSet, tableNames);
            }
        }

        public static void FillDataSet(string connectionString, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params DbParameter[] DbParameters)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (dataSet == null)
                throw new ArgumentNullException("dataSet");
            using (DbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                SqlHelper.FillDataSet(connection, commandType, commandText, dataSet, tableNames, DbParameters);
            }
        }

        public static void FillDataSet(string connectionString, string storedProcedureName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (dataSet == null)
                throw new ArgumentNullException("dataSet");
            using (DbConnection connection = new OleDbConnection(connectionString))
            {
                connection.Open();
                SqlHelper.FillDataSet(connection, storedProcedureName, dataSet, tableNames, parameterValues);
            }
        }

        public static void FillDataSet(DbConnection connection, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
        {
            SqlHelper.FillDataSet(connection, commandType, commandText, dataSet, tableNames, (DbParameter[])null);
        }

        public static void FillDataSet(DbConnection connection, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params DbParameter[] DbParameters)
        {
            SqlHelper.FillDataSet(connection, (DbTransaction)null, commandType, commandText, dataSet, tableNames, DbParameters);
        }

        public static void FillDataSet(DbConnection connection, string storedProcedureName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (dataSet == null)
                throw new ArgumentNullException("dataSet");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues != null && parameterValues.Length > 0)
            {
                DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
                SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
                SqlHelper.FillDataSet(connection, CommandType.StoredProcedure, storedProcedureName, dataSet, tableNames, spParameterSet);
            }
            else
                SqlHelper.FillDataSet(connection, CommandType.StoredProcedure, storedProcedureName, dataSet, tableNames);
        }

        public static void FillDataSet(DbTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames)
        {
            SqlHelper.FillDataSet(transaction, commandType, commandText, dataSet, tableNames, (DbParameter[])null);
        }

        public static void FillDataSet(DbTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params DbParameter[] DbParameters)
        {
            SqlHelper.FillDataSet(transaction.Connection, transaction, commandType, commandText, dataSet, tableNames, DbParameters);
        }

        public static void FillDataSet(DbTransaction transaction, string storedProcedureName, DataSet dataSet, string[] tableNames, params object[] parameterValues)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (dataSet == null)
                throw new ArgumentNullException("dataSet");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (parameterValues != null && parameterValues.Length > 0)
            {
                DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
                SqlHelper.AssignParameterValues(spParameterSet, parameterValues);
                SqlHelper.FillDataSet(transaction, CommandType.StoredProcedure, storedProcedureName, dataSet, tableNames, spParameterSet);
            }
            else
                SqlHelper.FillDataSet(transaction, CommandType.StoredProcedure, storedProcedureName, dataSet, tableNames);
        }

        private static void FillDataSet(DbConnection connection, DbTransaction transaction, CommandType commandType, string commandText, DataSet dataSet, string[] tableNames, params DbParameter[] DbParameters)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (dataSet == null)
                throw new ArgumentNullException("dataSet");
            OleDbCommand OleDbCommand = new OleDbCommand();
            bool mustCloseConnection = false;
            SqlHelper.PrepareCommand(OleDbCommand, connection, transaction, commandType, commandText, DbParameters, out mustCloseConnection);
            using (OleDbDataAdapter OleDbDataAdapter = new OleDbDataAdapter(OleDbCommand))
            {
                if (tableNames != null && tableNames.Length > 0)
                {
                    string sourceTable = "Table";
                    for (int index = 0; index < tableNames.Length; ++index)
                    {
                        if (tableNames[index] == null || tableNames[index].Length == 0)
                            throw new ArgumentException("The tableNames parameter must contain a list of tables, a value was provided as null or empty string.", "tableNames");
                        OleDbDataAdapter.TableMappings.Add(sourceTable, tableNames[index]);
                        sourceTable += (index + 1).ToString();
                    }
                }
                OleDbDataAdapter.Fill(dataSet);
                OleDbCommand.Parameters.Clear();
            }
            if (!mustCloseConnection)
                return;
            connection.Close();
        }

        public static void UpdateDataSet(OleDbCommand insertCommand, OleDbCommand deleteCommand, OleDbCommand updateCommand, DataSet dataSet, string tableName)
        {
            if (insertCommand == null)
                throw new ArgumentNullException("insertCommand");
            if (deleteCommand == null)
                throw new ArgumentNullException("deleteCommand");
            if (updateCommand == null)
                throw new ArgumentNullException("updateCommand");
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException("tableName");
            using (OleDbDataAdapter OleDbDataAdapter = new OleDbDataAdapter())
            {
                OleDbDataAdapter.UpdateCommand = updateCommand;
                OleDbDataAdapter.InsertCommand = insertCommand;
                OleDbDataAdapter.DeleteCommand = deleteCommand;
                OleDbDataAdapter.Update(dataSet, tableName);
                dataSet.AcceptChanges();
            }
        }

        public static OleDbCommand CreateCommand(OleDbConnection connection, string storedProcedureName, params string[] sourceColumns)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            OleDbCommand command = new OleDbCommand(storedProcedureName, connection);
            command.CommandType = CommandType.StoredProcedure;
            if (sourceColumns != null && sourceColumns.Length > 0)
            {
                DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
                for (int index = 0; index < sourceColumns.Length; ++index)
                    spParameterSet[index].SourceColumn = sourceColumns[index];
                SqlHelper.AttachParameters(command, spParameterSet);
            }
            return command;
        }

        public static int ExecuteNonQueryTypedParams(string connectionString, string storedProcedureName, DataRow dataRow)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteNonQuery(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static int ExecuteNonQueryTypedParams(DbConnection connection, string storedProcedureName, DataRow dataRow)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteNonQuery(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static int ExecuteNonQueryTypedParams(DbTransaction transaction, string storedProcedureName, DataRow dataRow)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteNonQuery(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DataSet ExecuteDataSetTypedParams(string connectionString, string storedProcedureName, DataRow dataRow)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteDataSet(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteDataSet(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DataSet ExecuteDataSetTypedParams(DbConnection connection, string storedProcedureName, DataRow dataRow)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteDataSet(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteDataSet(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DataSet ExecuteDataSetTypedParams(DbTransaction transaction, string storedProcedureName, DataRow dataRow)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteDataSet(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteDataSet(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DbDataReader ExecuteReaderTypedParams(string connectionString, string storedProcedureName, DataRow dataRow)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteReader(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DbDataReader ExecuteReaderTypedParams(DbConnection connection, string storedProcedureName, DataRow dataRow)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteReader(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteReader(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static DbDataReader ExecuteReaderTypedParams(DbTransaction transaction, string storedProcedureName, DataRow dataRow)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteReader(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteReader(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static object ExecuteScalarTypedParams(string connectionString, string storedProcedureName, DataRow dataRow)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connectionString, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteScalar(connectionString, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static object ExecuteScalarTypedParams(DbConnection connection, string storedProcedureName, DataRow dataRow)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteScalar(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteScalar(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static object ExecuteScalarTypedParams(DbTransaction transaction, string storedProcedureName, DataRow dataRow)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteScalar(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static XmlReader ExecuteXmlReaderTypedParams(DbConnection connection, string storedProcedureName, DataRow dataRow)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteXmlReader(connection, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }

        public static XmlReader ExecuteXmlReaderTypedParams(DbTransaction transaction, string storedProcedureName, DataRow dataRow)
        {
            if (transaction == null)
                throw new ArgumentNullException("transaction");
            if (transaction != null && transaction.Connection == null)
                throw new ArgumentException("The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
            if (string.IsNullOrEmpty(storedProcedureName))
                throw new ArgumentNullException("storedProcedureName");
            if (dataRow == null || dataRow.ItemArray.Length <= 0)
                return SqlHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, storedProcedureName);
            DbParameter[] spParameterSet = SqlHelperParameterCache.GetSpParameterSet(transaction.Connection, storedProcedureName);
            SqlHelper.AssignParameterValues(spParameterSet, dataRow);
            return SqlHelper.ExecuteXmlReader(transaction, CommandType.StoredProcedure, storedProcedureName, spParameterSet);
        }
    }
}
