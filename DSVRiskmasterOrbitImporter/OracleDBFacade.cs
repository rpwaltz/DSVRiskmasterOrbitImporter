using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
/**
    Risk Master Importer transfers data from RiskMaster DSV spreadsheets into Database Tables
    Copyright(C) 2020  City of Knoxville

    This program is free software: you can redistribute it and/or modify

	it under the terms of the GNU General Public License as published by

	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of

	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the

	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.If not, see<https://www.gnu.org/licenses/>.
**/


namespace runnerDotNet
    {
    public class OracleDBFacade
        {
        IDictionary<string, string> _pfDSVColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _pdDSVColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _xxcokRiskMasterPaymentsColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _apSupplierSitesAllColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _appsXxcokApSuppliersVwColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _xxcokRiskmasterPaymentError = new Dictionary<string, string>();
        IDictionary<string, string> _pfPKDSVColumnDataTypesDictionary = new Dictionary<string, string>();
        IDictionary<string, string> _pdPKDSVColumnDataTypesDictionary = new Dictionary<string, string>();

        public OracleDBFacade()
            {
            _pfDSVColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pfDSVColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("CONTROL_NUMBER", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CHECK_NUMBER", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("PAYEE_NAME", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("STREET_ADDRESS_1", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("STREET_ADDRESS_2", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CITY", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("STATE", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("ZIP_CODE", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CHECK_MEMO", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CLAIM_NUMBER", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CLAIMANT_NAME", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CHECK_AMOUNT", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("CHECK_DATE", "DATE");
            _pfDSVColumnDataTypesDictionary.Add("TRANSACTION_DATE", "DATE");
            _pfDSVColumnDataTypesDictionary.Add("STATUS_CODE", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("PAYMENT_FLAG", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("CLEARED_FLAG", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("CLAIM_ID", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("INVOICE_DATE", "DATE");
            _pfDSVColumnDataTypesDictionary.Add("CLAIMANT_FIRST_NAME", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("CLAIMANT_LAST_NAME", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("SUBSTR(CLAIMANT_ABBREVIATION,0,10)", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("NPI_NUMBER", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("TITLE", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("IS_VALID", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("IS_PROCESSED", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("IS_EDI_PAYMENT", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("UPDATED_BY_USER", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("ADDED_BY_USER", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("TIN", "STRING");
            _pfDSVColumnDataTypesDictionary.Add("BATCH_ID", "NUMBER");
            _pfDSVColumnDataTypesDictionary.Add("ORIG_SUPPLIER_NUMBER", "STRING");

            _pfPKDSVColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pfPKDSVColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pfPKDSVColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");

            _pdDSVColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pdDSVColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pdDSVColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");
            _pdDSVColumnDataTypesDictionary.Add("PAYMENT_DETAIL_ID", "NUMBER");
            _pdDSVColumnDataTypesDictionary.Add("TRANSACTION_TYPE", "STRING");
            _pdDSVColumnDataTypesDictionary.Add("SPLIT_AMOUNT", "NUMBER");
            _pdDSVColumnDataTypesDictionary.Add("GENERAL_LEDGER_ACCOUNT", "STRING");
            _pdDSVColumnDataTypesDictionary.Add("FROM_DATE", "DATE");
            _pdDSVColumnDataTypesDictionary.Add("TO_DATE", "DATE");
            _pdDSVColumnDataTypesDictionary.Add("INVOICED_BY", "STRING");
            _pdDSVColumnDataTypesDictionary.Add("INVOICE_AMOUNT", "NUMBER");
            _pdDSVColumnDataTypesDictionary.Add("INVOICE_NUMBER", "STRING");
            _pdDSVColumnDataTypesDictionary.Add("PO_NUMBER", "STRING");

            _pdPKDSVColumnDataTypesDictionary.Add("IMPORT_ID", "STRING");
            _pdPKDSVColumnDataTypesDictionary.Add("IMPORT_DATE", "DATE");
            _pdPKDSVColumnDataTypesDictionary.Add("PAYMENT_ID", "NUMBER");
            _pdPKDSVColumnDataTypesDictionary.Add("PAYMENT_DETAIL_ID", "NUMBER");

            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CTL_NUMERIC", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CLAIM_ID", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CLAIM_NUMERIC", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("NAME", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ADDR1", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ADDR2", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CITY", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("STATE", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ZIP_CODE", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("AMOUNT", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("TRANS_DATE", "DATE");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("INVOICE_NUMERIC", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("INVOICE_DATE", "DATE");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("TRANSACTION_TYPE_DESCR", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("DEPT", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("LINE_OF_BUS_CODE", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("DEPT_ASSIGNED_EID", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("DEPT_DESCR", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("SUPPLIER_NBR", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("VENDOR_SITE_CODE", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("INVOICE_TOTAL", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("TRANS_NUMERIC", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("PRIMARY_KEY", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("CREATED_BY", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("IS_EDI_PAYMENT", "NUMBER");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("UPDATED_BY_USER", "STRING");
            _xxcokRiskMasterPaymentsColumnDataTypesDictionary.Add("ADDED_BY_USER", "STRING");

            _apSupplierSitesAllColumnDataTypesDictionary.Add("VENDOR_SITE_CODE", "STRING");
            _apSupplierSitesAllColumnDataTypesDictionary.Add("VENDOR_ID", "NUMBER");
            _apSupplierSitesAllColumnDataTypesDictionary.Add("PAY_SITE_FLAG", "STRING");

            _appsXxcokApSuppliersVwColumnDataTypesDictionary.Add("VENDOR_ID", "NUMBER");

            _xxcokRiskmasterPaymentError.Add("IMPORT_ID", "STRING");
            _xxcokRiskmasterPaymentError.Add("IMPORT_DATE", "DATE");
            _xxcokRiskmasterPaymentError.Add("PAYMENT_ID", "NUMBER");
            _xxcokRiskmasterPaymentError.Add("ERROR_ID", "NUMBER");
            _xxcokRiskmasterPaymentError.Add("ERROR_DESCR", "STRING");
            }

        public dynamic Select(dynamic _param_table, dynamic _param_userConditions = null)
            {
            string whereClause = "";
            StringBuilder selectStatement = new StringBuilder();
            if (_param_userConditions is string)
                {
                whereClause = _param_userConditions;
                }
            else
                {
                if (_param_table.Equals("xxcok.xxcok_rm_import_pf"))
                    {

                    whereClause = buildWhereClauseColumnNamesAndValues(this._pfDSVColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("xxcok.xxcok_risk_master_payments"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._xxcokRiskMasterPaymentsColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("ap.ap_supplier_sites_all"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._apSupplierSitesAllColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("apps.xxcok_ap_suppliers_vw"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._appsXxcokApSuppliersVwColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                }
            selectStatement.AppendFormat("SELECT * FROM {0} WHERE {1}", _param_table, whereClause);
            QueryResult queryResult = DB.Query(selectStatement.ToString());
            return queryResult;
            }
        public dynamic Delete(dynamic _param_table, dynamic _param_userConditions = null)
            {
            XVar deleteResult = new XVar();
            string whereClause = "";
            StringBuilder deleteStatement = new StringBuilder();
            if (_param_userConditions is string)
                {
                whereClause = _param_userConditions;
                }
            else
                {
                if (_param_table.Equals("xxcok.xxcok_rm_import_pf"))
                    {

                    whereClause = buildWhereClauseColumnNamesAndValues(this._pfPKDSVColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                else if (_param_table.Equals("xxcok.xxcok_rm_import_pd"))
                    {
                    whereClause = buildWhereClauseColumnNamesAndValues(this._pdPKDSVColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }

                }
            deleteStatement.AppendFormat("DELETE FROM {0} WHERE {1}", _param_table, whereClause);

            DB.Exec(deleteStatement.ToString());
            return deleteResult;
            }
        public dynamic Exec(dynamic _param_sql)
            {

            dynamic queryResult = DB.Exec(_param_sql);
            return queryResult;
            }
        public dynamic Query(dynamic _param_sql)
            {

            dynamic queryResult = DB.Query(_param_sql);
            return queryResult;
            }
        public XVar Insert(string tableName, dynamic data)
            {
            XVar insertResult = new XVar();
            string[] columnAndValues = new string[0];
            StringBuilder insertStatement = new StringBuilder();
            if (tableName.Equals("xxcok.xxcok_rm_import_pf"))
                {
                columnAndValues = buildInsertColumnNamesAndValues(this._pfDSVColumnDataTypesDictionary, (XVar)data);

                }
            else if (tableName.Equals("xxcok.xxcok_rm_import_pd"))
                {
                columnAndValues = buildInsertColumnNamesAndValues(this._pdDSVColumnDataTypesDictionary, (XVar)data);
                }
            if (columnAndValues.Length == 0)
                {
                throw new Exception("tablename is all wrong: " + tableName);
                }


            insertStatement.AppendFormat("INSERT INTO {0} ({1}) VALUES ({2})", tableName, columnAndValues[0], columnAndValues[1]);
            DSVRiskmasterOrbitImporter.Program.outputLines.AppendLine(insertStatement.ToString());
            DB.Exec(insertStatement.ToString());

            return insertResult;
            }
        public XVar Update(dynamic _param_table, dynamic _param_data, dynamic _param_userConditions, dynamic whereClause = null)
            {
            XVar updateResult = new XVar();
            string[] columnAndValues = new string[0];
            StringBuilder updateStatement = new StringBuilder();
            if (_param_table.Equals("xxcok.xxcok_rm_import_pf"))
                {
                columnAndValues = buildUpdateColumnNamesAndValues(this._pfDSVColumnDataTypesDictionary, (XVar)_param_data);
                if (whereClause == null || String.IsNullOrEmpty(whereClause.ToString())) 
                    { 
                    whereClause = buildWhereClauseColumnNamesAndValues(this._pfDSVColumnDataTypesDictionary, (XVar)_param_userConditions);
                    }
                }
            if (columnAndValues.Length == 0)
                {
                throw new Exception("tablename is all wrong: " + _param_table);
                }


            updateStatement.AppendFormat("UPDATE {0} SET {1} WHERE {2}", _param_table, columnAndValues[0], whereClause);
            DB.Exec(updateStatement.ToString());

            return updateResult;
            }
        static public XVar buildPfImportRecordPrimaryKeyClause(dynamic record)
            {
            XVar recordWhereClause = new XVar();
            
            XVar value = record["IMPORT_ID"];

                if (value != null && value.IsString() && (value.ToString().Length > 0))
                    {
                    recordWhereClause.SetArrayItem("IMPORT_ID", value);
                    }
                else
                    {
                    throw new NullReferenceException("IMPORT_ID was null in a data retreived from non null column");
                    }
                

             value = record["PF_ID"];
                
                if (value != null && value.IsNumeric())
                    {
                    recordWhereClause.SetArrayItem("PF_ID", value);
                    }
                else
                    {
                    throw new NullReferenceException("PF_ID was null in a data retreived from non null column");
                    }
                
            value = record["IMPORT_DATE"];
                
                if (value != null && value.Value is DateTime)
                    {
                    DateTime import_datetime = (DateTime)value.Value;
                    recordWhereClause.SetArrayItem("IMPORT_DATE", import_datetime.ToString("yyyyMMdd"));
                    }
                else
                    {
                    throw new NullReferenceException("IMPORT_DATE was null in a data retreived from non null column");
                    }
                

                value = record["PAYMENT_ID"];
                
                if (value != null && value.IsNumeric())
                    {
                    recordWhereClause.SetArrayItem("PAYMENT_ID", value.ToString());

                    }
                else
                    {
                    throw new NullReferenceException("PAYMENT_ID was null in a data retreived from non null column");
                    }
              
            return recordWhereClause;
            }

        static public XVar buildWCEDIPrimaryKeyClause(dynamic record)
            {
            XVar recordWhereClause = new XVar();
            XVar value = record["IMPORT_ID"];

            if (value != null && value.IsString() && (value.ToString().Length > 0))
                {
                recordWhereClause.SetArrayItem("IMPORT_ID", value);
                }
            else
                {
                throw new NullReferenceException("IMPORT_ID was null in a data retreived from non null column");
                }


            value = record["PD_ID"];

            if (value != null && value.IsNumeric())
                {
                recordWhereClause.SetArrayItem("PD_ID", value);
                }
            else
                {
                throw new NullReferenceException("PF_ID was null in a data retreived from non null column");
                }

            value = record["IMPORT_DATE"];

            if (value != null && value.Value is DateTime)
                {
                DateTime import_datetime = (DateTime)value.Value;
                recordWhereClause.SetArrayItem("IMPORT_DATE", import_datetime.ToString("yyyyMMdd"));
                }
            else
                {
                throw new NullReferenceException("IMPORT_DATE was null in a data retreived from non null column");
                }


            value = record["PAYMENT_ID"];

            if (value != null && value.IsNumeric())
                {
                recordWhereClause.SetArrayItem("PAYMENT_ID", value.ToString());

                }
            else
                {
                throw new NullReferenceException("PAYMENT_ID was null in a data retreived from non null column");
                }

            return recordWhereClause;
            }


        private string[] buildInsertColumnNamesAndValues(IDictionary<string, string> DSVColumnDataTypesDictionary, XVar data)
            {
            string[] returnStrings = new string[2];
            StringBuilder DSVColumns = new StringBuilder();
            StringBuilder DSVValues = new StringBuilder();
            // XXX THIS IS REALLY INEFFICIENT, IT IS BACKWARDS
            foreach (KeyValuePair<string, string> kvp in DSVColumnDataTypesDictionary)
                {
                if (data.KeyExists(kvp.Key))
                    {
                    if (DSVColumns.Length > 0)
                        {
                        DSVColumns.Append(",");
                        DSVValues.Append(",");
                        }
                    DSVColumns.Append(kvp.Key);
                    if (data.GetArrayItem(kvp.Key).Length() == 0)
                        {
                        DSVValues.Append("NULL");
                        }
                    else if (kvp.Value.Equals("STRING"))
                        {
                        string valueString = data.GetArrayItem(kvp.Key).Replace("'", "''");
                        DSVValues.AppendFormat("{0}{1}{2}", "'", valueString, "'");
                        }
                    else if (kvp.Value.Equals("NUMBER"))
                        {
                        DSVValues.Append(data.GetArrayItem(kvp.Key).ToString());
                        }
                    else if (kvp.Value.Equals("DATE"))
                        {
                        DSVValues.AppendFormat("{0}{1}{2}", "TO_DATE('", data.GetArrayItem(kvp.Key).ToString(), "', 'YYYYMMDD')");
                        // DSVValues.AppendFormat("{0}{1}{2}", "'", data[kvp.Key], "'");
                        }
                    else
                        {
                        throw new Exception("Could not file datatype of " + kvp.Value);
                        }
                    }
                }

            returnStrings[0] = DSVColumns.ToString();
            returnStrings[1] = DSVValues.ToString();
            return returnStrings;
            }
        private string[] buildUpdateColumnNamesAndValues(IDictionary<string, string> DSVColumnDataTypesDictionary, XVar data)
            {
            string[] returnStrings = new string[1];
            StringBuilder updateStmtBuilder = new StringBuilder();
            // XXX THIS IS REALLY INEFFICIENT, IT IS BACKWARDS
            int loop = 0;
            foreach (KeyValuePair<string, string> kvp in DSVColumnDataTypesDictionary)
                {
                if (data.KeyExists(kvp.Key))
                    {
                    if (loop > 0)
                        {
                        updateStmtBuilder.Append(",");
                        }
                    if (data.GetArrayItem(kvp.Key).Length() == 0)
                        {
                        updateStmtBuilder.AppendFormat("{0} = NULL", kvp.Key);
                        }
                    else if (kvp.Value.Equals("STRING"))
                        {
                        string valueString = data.GetArrayItem(kvp.Key).Replace("'", "''");
                        updateStmtBuilder.AppendFormat("{0}={1}{2}{3}", kvp.Key, "'", valueString, "'");
                        }
                    else if (kvp.Value.Equals("NUMBER"))
                        {
                        updateStmtBuilder.AppendFormat("{0}={1}", kvp.Key, data.GetArrayItem(kvp.Key).ToString());
                        }
                    else if (kvp.Value.Equals("DATE"))
                        {
                        updateStmtBuilder.AppendFormat("{0} = {1}{2}{3}", kvp.Key, "TO_DATE('", data.GetArrayItem(kvp.Key).ToString(), "', 'YYYYMMDD')");
                        }
                    else
                        {
                        throw new Exception("Could not file datatype of " + kvp.Value);
                        }
                    loop++;
                    }

                }

            returnStrings[0] = updateStmtBuilder.ToString();

            return returnStrings;
            }
        private string buildWhereClauseColumnNamesAndValues(IDictionary<string, string> DSVColumnDataTypesDictionary, XVar data)
            {
            string returnString = "";
            StringBuilder whereStmtBuilder = new StringBuilder();
            // XXX THIS IS REALLY INEFFICIENT, IT IS BACKWARDS
            foreach (KeyValuePair<string, string> kvp in DSVColumnDataTypesDictionary)
                {
                if (data.KeyExists(kvp.Key))
                    {
                    if (whereStmtBuilder.Length > 0)
                        {
                        whereStmtBuilder.Append(" AND ");
                        }
                    if (kvp.Value.Equals("STRING"))
                        {
                        string valueString = data[kvp.Key].Replace("'", "''");
                        whereStmtBuilder.AppendFormat("{0} = {1}{2}{3}", kvp.Key, "'", valueString, "'");
                        }
                    else if (kvp.Value.Equals("NUMBER"))
                        {
                        whereStmtBuilder.AppendFormat("{0} = {1}", kvp.Key, data[kvp.Key]);
                        }
                    else if (kvp.Value.Equals("DATE"))
                        {
                        whereStmtBuilder.AppendFormat("{0} = {1}{2}{3}", kvp.Key, "TO_DATE('", data[kvp.Key], "', 'YYYYMMDD')");
                        }
                    else
                        {
                        throw new Exception("Could not file datatype of " + kvp.Value);
                        }
                    }
                }
            returnString = whereStmtBuilder.ToString();

            return returnString;
            }

        }
    }
