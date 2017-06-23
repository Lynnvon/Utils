using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Windows.Forms;

namespace Utils.SQL
{
    /// <summary>
    /// Access操作类
    /// </summary>
    public static class DataOperate
    {
        private static readonly string StrCon = ConfigurationManager.AppSettings["connectionString"];

        public static OleDbConnection GetConnection()
        {
            return new OleDbConnection(StrCon);
        }

        public static int ExecDataBySql(string strSql)
        {
            return SqlHelper.ExecuteNonQuery(GetConnection(), CommandType.Text, strSql);
        }

        public static bool ExecDataBySqls(List<string> strSqls)
        {
            bool isSucceed;

            DbConnection connection = GetConnection();
            DbCommand command = new OleDbCommand();
            command.Connection = connection;
            connection.Open();
            DbTransaction transaction = connection.BeginTransaction();
            try
            {
                command.Transaction = transaction;
                foreach (string strSql in strSqls)
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = strSql;
                    command.ExecuteNonQuery();
                }
                transaction.Commit();
                isSucceed = true;
            }
            catch (Exception)
            {
                transaction.Rollback();
                isSucceed = false;
            }
            finally
            {
                connection.Close();
            }
            return isSucceed;
        }

        public static DataSet GetDataSet(string strSql)
        {
            return SqlHelper.ExecuteDataSet(GetConnection(), CommandType.Text, strSql);
        }

        public static DbDataReader GetDataReader(string strSql)
        {
            return SqlHelper.ExecuteReader(GetConnection(), CommandType.Text, strSql);
        }

        public static object GetSingleObject(string strSql)
        {
            return SqlHelper.ExecuteScalar(GetConnection(), CommandType.Text, strSql);
        }

        public static DataTable GetDataTable(string strSql)
        {
            return SqlHelper.ExecuteDataSet(GetConnection(), CommandType.Text, strSql).Tables[0];
        }

        public static DataTable GetDataTable(string procedureName, DbParameter[] sqlParameters)
        {
            return
                SqlHelper.ExecuteDataSet(GetConnection(), CommandType.StoredProcedure, procedureName, sqlParameters).
                    Tables[0];
        }
    }
}