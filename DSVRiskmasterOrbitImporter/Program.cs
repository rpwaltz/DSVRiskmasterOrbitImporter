using runnerDotNet;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSVRiskmasterOrbitImporter
    {
    class Program
        {
        public static System.Text.StringBuilder outputLines = new System.Text.StringBuilder();
        static void Main(string[] args)
            {
            
            try
                {

                ExeConfigurationFileMap map = new ExeConfigurationFileMap { ExeConfigFilename = @"F:\RiskMasterImports\config\orbitimportroutines.conf" };
                Configuration config = ConfigurationManager.OpenMappedExeConfiguration(map, ConfigurationUserLevel.None);
                string processDirectory = config.AppSettings.Settings["DSVProcessDirectory"].Value;
                outputLines.AppendLine("Starting DSVRiskmasterOrbitImporter");

                string archiveDirectory = config.AppSettings.Settings["DSVArchiveDirectory"].Value;

                string failDirectory = config.AppSettings.Settings["DSVFailDirectory"].Value;
                string DSVLogDirectory = config.AppSettings.Settings["DSVLogDirectory"].Value;

                char DSVDelimiter = ',';
                char DSVQualifier = '"';

                
                /*  Initial Import of Records
                 * 
                 * 
                // RiskMasterImporter is a City of Knoxville custom built class created by Robert Waltz
                // RiskMasterImporter is found in  ASP_OrbitImportRoutines\source\include
                */

                RiskMasterImporter riskMasterImporter = new RiskMasterImporter(processDirectory, archiveDirectory, failDirectory, DSVDelimiter, DSVQualifier);
                try
                    {
                    //PF files contain the parent relationship to PD files. Parse the PF files first

                    bool success = riskMasterImporter.IterateParseAndInsertCvsFiles();

                    IList<String> failedPfFiles = riskMasterImporter.FailedPfFiles;
                    int failedPfFilesCount = failedPfFiles.Count;
                    if (failedPfFilesCount > 0)
                        {
                        outputLines.AppendFormat(@"PF Processing failed for {0} file{1} {2}", failedPfFilesCount, failedPfFilesCount > 1 ? "s" : "", System.Environment.NewLine);
                        outputLines.AppendLine(RiskMasterImporter.ErrorStringBuilder.ToString());
                        }
                    int successPfFileCount = riskMasterImporter.ProcessedPfFiles.Count;
                    if (successPfFileCount > 0)
                        {
                        outputLines.AppendFormat(@"PF Processing succeeded for  {0} file{1} {2}", successPfFileCount, successPfFileCount > 1 ? "s" : "", System.Environment.NewLine);
                        }
                    int failedPDFilesCount = riskMasterImporter.FailedPdFiles.Count;
                    if (failedPDFilesCount > 0)
                        {

                        outputLines.AppendFormat(@"PD Processing failed for {0} file{1} {2}", failedPDFilesCount, failedPDFilesCount > 1 ? "s" : "", System.Environment.NewLine);

                        }
                    int successPdFileCount = riskMasterImporter.ProcessedPdFiles.Count;
                    if (successPdFileCount > 0)
                        {
                        outputLines.AppendFormat(@"PD Processing succeeded for  {0} file{1} {2}", successPdFileCount, successPdFileCount > 1 ? "s" : "", System.Environment.NewLine);
                        }
                    if (RiskMasterImporter.ErrorStringBuilder.Length > 0)
                        {
                        outputLines.Append(RiskMasterImporter.ErrorStringBuilder);
                        }
                    }
                catch (System.Exception e)
                    {
                    outputLines.AppendFormat(" An exception ({0}) occurred {1}", e.GetType().Name, System.Environment.NewLine);
                    outputLines.AppendFormat(" Message:  {0} {1}", e.Message, System.Environment.NewLine);
                    outputLines.AppendFormat(" Stack Trace:  {0} {1}", e.StackTrace, System.Environment.NewLine);
                    System.Exception ie = e.InnerException;
                    if (ie != null)
                        {
                        outputLines.AppendFormat(" The Inner Exception: {0}", System.Environment.NewLine);
                        outputLines.AppendFormat(" Exception Name: {0} {1}", ie.GetType().Name, System.Environment.NewLine);
                        outputLines.AppendFormat("  Message: {0} {1}", ie.Message, System.Environment.NewLine);
                        outputLines.AppendFormat(" Stack Trace: {0 {1} {2}", System.Environment.NewLine, ie.StackTrace, System.Environment.NewLine);
                        }
                    }
                /*
                OracleDBFacade oracleDBFacade = new OracleDBFacade();
                List<string> selectedRecords = new List<string>();

                String wipIdSelectStatementStatement = "SELECT PF_ID FROM  xxcok.xxcok_rm_import_pf WHERE IS_VALID = 0 AND IS_PROCESSED = 0";

                dynamic queryResult = oracleDBFacade.Query(wipIdSelectStatementStatement);

                //&& queryResult.Count() > 0
                if (queryResult != null)
                    {
                    XVar record = queryResult.fetchAssoc();
                    while (record != null && !string.IsNullOrEmpty(record.GetArrayItem("PF_ID")))
                        {
                        dynamic value = record.GetArrayItem("PF_ID");
                        
                        string pf_id = value.ToString();
                        if (!String.IsNullOrEmpty(pf_id))
                            {
                            
                            selectedRecords.Add(pf_id);
                            }
                        else
                            {
                            outputLines.AppendLine("Value of record[\"PF_ID\"] is null or empty.");
                            }
                        record = queryResult.fetchAssoc();
                        }

                    }
                else
                    {
                    NullReferenceException nullReferenceException = new NullReferenceException("query result is null.");
                    throw nullReferenceException;
                    }

                string currentDateTimeString = DateTime.Now.ToString("u", System.Globalization.CultureInfo.CreateSpecificCulture("en-US"));
                // CREATE THE BATCH
                int batch_id = -1;
                String insertStatement = String.Format("INSERT INTO XXCOK.XXCOK_RM_BATCH (BATCH_DATE, RECORD_COUNT, PAYMENT_TOTAL) VALUES (TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS\"Z\"'), -1, -1)", currentDateTimeString);
                DB.Exec(insertStatement.ToString());

                String batchIdSelectStatementStatement = String.Format("SELECT BATCH_ID FROM  XXCOK.XXCOK_RM_BATCH WHERE BATCH_DATE = TO_DATE('{0}', 'YYYY-MM-DD HH24:MI:SS\"Z\"')", currentDateTimeString);

                queryResult = oracleDBFacade.Query(batchIdSelectStatementStatement.ToString());

                if (queryResult != null)
                    {
                    dynamic record = queryResult.fetchAssoc();
                    if (record != null && !string.IsNullOrEmpty(record["BATCH_ID"]))
                        {
                        dynamic value = record["BATCH_ID"];
                        if (!String.IsNullOrEmpty(value))
                            {
                            batch_id = int.Parse(value.ToString());
                            }
                        else
                            {
                            NullReferenceException nullReferenceException = new NullReferenceException("Value of record[\"BATCH_ID\"] is null or empty. [" + value.ToString() + "]");
                            throw nullReferenceException;
                            }

                        }
                    else
                        {
                        NullReferenceException nullReferenceException = new NullReferenceException("Value of record[\"BATCH_ID\"] is null or empty or not a string." + record.ToString());
                        throw nullReferenceException;
                        }
                    }
                else
                    {
                    Exception noQueryResults = new Exception("BATCH COULD NOT BE CREATED by the insert " + insertStatement.ToString());
                    throw noQueryResults;
                    }
                string formatUpdateBatchIdString = @"update xxcok.xxcok_rm_import_pf pf set BATCH_ID = {0} WHERE PF_ID in ({1}) and (BATCH_ID IS NULL OR BATCH_ID = 0)";
                String inUpdateBatchIdClause = String.Join(",", selectedRecords);
                String updateUpdateBatchIdStmt = string.Format(formatUpdateBatchIdString, batch_id, inUpdateBatchIdClause);
                DB.Exec(updateUpdateBatchIdStmt);
                // Place event code here.
                RiskMasterWCEDIPayments exporter = new RiskMasterWCEDIPayments(batch_id, config);
                try
                    {
                    if (selectedRecords.Count > 0)
                        {

                        exporter.Validate(selectedRecords);
                        if (exporter.ErrorStringBuilder.Length > 0)
                            {
                            outputLines.Append("RiskMasterWCEDIPayments Validate INFO:" + System.Environment.NewLine + exporter.InfoStringBuilder.ToString());
                            outputLines.Append("RiskMasterWCEDIPayments Validate ERROR:" + System.Environment.NewLine + exporter.ErrorStringBuilder.ToString());
                            }
                        else
                            {

                            exporter.SendToWCEDIAndForOrbitApproval(@"\\knoxlan\infosys\apps\rwaltz\WCEDI_Exports\PrimeEOR\Batch366.pdf");
                            outputLines.Append("RiskMasterWCEDIPayments SendToWCEDIAndForOrbitApproval INFO:" + System.Environment.NewLine + exporter.InfoStringBuilder.ToString());
                            outputLines.Append("RiskMasterWCEDIPayments SendToWCEDIAndForOrbitApproval ERROR:" + System.Environment.NewLine + exporter.ErrorStringBuilder.ToString());
                            }

                        }
                    else
                        {
                        outputLines.AppendLine("NO RECORDS TO VALIDATE");
                        }

                    }
                catch (System.Exception e)
                    {
                    outputLines.Append("RiskMasterWCEDIPayments Validate INFO:" + System.Environment.NewLine + exporter.InfoStringBuilder.ToString());
                    outputLines.Append("RiskMasterWCEDIPayments Validate ERROR:" + System.Environment.NewLine + exporter.ErrorStringBuilder.ToString());
                    outputLines.AppendFormat(e.ToString());
                    outputLines.AppendFormat("<font color=\'red\'>An exception ({0}) occurred.</font> <br />{1}", e.GetType().Name, System.Environment.NewLine);
                    outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp; Message:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />{1}", e.Message, System.Environment.NewLine);
                    outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp; Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />{1}", e.StackTrace, System.Environment.NewLine);
                    System.Exception ie = e.InnerException;
                    if (ie != null)
                        {
                        outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp The Inner Exception:</font> <br />{0}", System.Environment.NewLine);
                        outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp&nbsp Exception Name: {0}</font> <br />{1}", ie.GetType().Name, System.Environment.NewLine);
                        outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Message: {0}</font> <br />{1}", ie.Message, System.Environment.NewLine);
                        outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp;&nbsp {0}</font> <br />{1}", ie.StackTrace, System.Environment.NewLine);
                        }
                    }

                    */
                }
            catch (System.Exception e)
                {
                outputLines.AppendFormat(e.ToString());
                outputLines.AppendFormat("<font color=\'red\'>An exception ({0}) occurred.</font> <br />{1}", e.GetType().Name, System.Environment.NewLine);
                outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp; Message:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />{1}", e.Message, System.Environment.NewLine);
                outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp; Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp; {0}</font> <br />{1}", e.StackTrace, System.Environment.NewLine);
                System.Exception ie = e.InnerException;
                if (ie != null)
                    {
                    outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp The Inner Exception:</font> <br />{0}", System.Environment.NewLine);
                    outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp&nbsp Exception Name: {0}</font> <br />{1}", ie.GetType().Name, System.Environment.NewLine);
                    outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Message: {0}</font> <br />{1}", ie.Message, System.Environment.NewLine);
                    outputLines.AppendFormat("<font color=\'red\'>&nbsp;&nbsp;&nbsp Stack Trace:</font> <br /><font color=\'red\'>&nbsp;&nbsp;&nbsp {0}</font> <br />{1}", ie.StackTrace, System.Environment.NewLine);
                    }
                }

            string baseLogFilename = "WCEDI_Exports";
            string logDirectory = @"F:\WCEDI_Exports\logs";
            outputLines.AppendLine("Completed Program");
            RiskmasterImportLogger logger = new RiskmasterImportLogger(logDirectory, baseLogFilename);

            logger.log(outputLines);

            }

        }


    }
