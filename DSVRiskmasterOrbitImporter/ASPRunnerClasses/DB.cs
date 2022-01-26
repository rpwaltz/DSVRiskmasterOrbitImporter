using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {
    class DB
        {
        private static String connectionString = "User Id=XXXX;Password=XXXX;Data Source=XXXXXX";

        public static dynamic Query(dynamic strSQL)
            {
            Oracle.ManagedDataAccess.Client.OracleConnection connection = new Oracle.ManagedDataAccess.Client.OracleConnection();
            connection.ConnectionString = DB.connectionString;
            connection.Open();

            Oracle.ManagedDataAccess.Client.OracleCommand dbCommand = new Oracle.ManagedDataAccess.Client.OracleCommand(strSQL, connection);
            dbCommand.CommandType = System.Data.CommandType.Text;
            try
                {

                dbCommand.Prepare();
                string commandStr = strSQL.ToLower().Substring(0, 6);
                string[] stopCommandList = { "insert", "update", "delete", "create", "drop", "rename", "alter" };
                if (stopCommandList.Any(x => commandStr.Substring(0, x.Length) == x))
                    {
                    dbCommand.ExecuteNonQuery();
                    dbCommand.Dispose();
                    return null;
                    }
                else
                    {

                    RunnerDBReader rdr = dbCommand.ExecuteReader();
                    rdr.Connection = dbCommand.Connection;

                    return new QueryResult(dbCommand.Connection, rdr);
                    }
                }
            catch (Exception e)
                {
                if (dbCommand != null)
                    dbCommand.Connection.Close();
                throw e;
                }

            }
        public static XVar Exec(dynamic strSQL)
            {


            Oracle.ManagedDataAccess.Client.OracleConnection connection = new Oracle.ManagedDataAccess.Client.OracleConnection();
            connection.ConnectionString = DB.connectionString;
            connection.Open();
            Oracle.ManagedDataAccess.Client.OracleCommand dbCommand = new Oracle.ManagedDataAccess.Client.OracleCommand(strSQL, connection);
            dbCommand.CommandType = System.Data.CommandType.Text;
            dbCommand.ExecuteNonQuery();
            dbCommand.Dispose();
            dbCommand = null;
            connection.Close();
            return null;

            }
        }
    }
