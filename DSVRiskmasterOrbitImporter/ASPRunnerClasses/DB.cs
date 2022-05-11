using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {
    class DB
        {

         public static string connectionString = "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=devux.server.knx)(PORT=1528))(CONNECT_DATA=(SERVICE_NAME=TSTEBS1)));User Id=import;Password=import";
        // public static string connectionString = "Data Source=TSTEBS1.WORLD;User Id=import;Password=import";
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
