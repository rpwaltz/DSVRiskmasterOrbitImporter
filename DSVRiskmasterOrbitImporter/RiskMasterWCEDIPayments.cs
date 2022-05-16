using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;

namespace runnerDotNet
    {
    public class RiskMasterWCEDIPayments
        {

        /*
         * 
         * 
        1. validate the records as above, validation runs from RiskmasterOrbitPayments class, validation method.
        2. update the records with the batch ID, NPI and TITLE, and the indicator 'IS_EDI_PAYMENT'
        4. Break the Uploaded Prime into PDF Pages
        5. Confirm all the pages in the WCEDI file are part of the records to be processed.Write to folder.
        6. populate WCEDIFile. Write to folder
        7. Zip page files
        8. FTP WCEDI txt file and PDF/Image zip file to WCEDI
        9. Populate Riskmaster Payments table.
        10. Send email
         *
         *
         */

        OracleDBFacade oracleDBFacade = new OracleDBFacade();

        IList<XVar> insertErrorList = new List<XVar>();
        private System.Text.StringBuilder _errorStringBuilder = new System.Text.StringBuilder();
        private System.Text.StringBuilder _infoStringBuilder = new System.Text.StringBuilder();
        private static string formatWCEDIPaymentsWhereString = @" pd_id in (select pd.pd_id from xxcok.xxcok_rm_import_pf pf inner join xxcok.xxcok_rm_import_pd pd 
on pf.import_id = pd.import_id and pf.import_date = pd.import_date and pf.payment_id = pd.payment_id where pf.IS_EDI_PAYMENT = 1 and pf.BATCH_ID = {0})";

        private readonly static string formatUpdatePfIsEDIPaymentString = @"
                update xxcok.xxcok_rm_import_pf pf
                set IS_EDI_PAYMENT = 1,
                    NPI_NUMBER= {0},
                    TITLE = '{1}'
                WHERE  IS_PROCESSED = 0 
                    AND BATCH_ID = {2}
                    AND PF_ID in ({3})";

        private readonly static string selectEDIInvoiceNumberString = @"
                select pf.pf_ID as PF_ID, pd.pd_ID as PD_ID, pd.INVOICE_NUMBER as INVOICE_NUMBER, pf.NPI_NUMBER as NPI_NUMBER, pf.TITLE as TITLE,
                                    pf.CONTROL_NUMBER as CONTROL_NUMBER,
                                    pf.CLAIM_ID as CLAIM_ID,
                                    pf.CLAIM_NUMBER as CLAIM_NUMBER,
                                    pf.PAYEE_NAME as PAYEE_NAME,
                                    pd.SPLIT_AMOUNT as SPLIT_AMOUNT,
                                    pf.TRANSACTION_DATE as TRANSACTION_DATE,
                                    pf.CHECK_NUMBER as CHECK_NUMBER
                from xxcok.xxcok_rm_import_pf pf inner join xxcok.xxcok_rm_import_pd pd 
                    on pf.import_id = pd.import_id and pf.import_date = pd.import_date and pf.payment_id = pd.payment_id
                WHERE  IS_PROCESSED = 0 
                    AND PF_ID in ({0})";

        private static string wcediHeaderRow = "OrderID|OrderAmt|OrderRef|PayerID|PayerTIN|PayerName|PayerRef|PayerAmt|FundingCode|PayeeID|PayeeTIN|PayeeName|PayeeNPI|PayeeAddress1|PayeeAddress2|PayeeCity|PayeeState|PayeeZipCode|PayeeFaxNumber|PayeeEmail|PayeeRef|PayeeDate|PayeeAmt|PayeeServiceDateFrom|PayeeServiceDateTo|ClaimantID|ClaimantTIN|ClaimantLastName|ClaimantFirstName|ClaimantAddress|ClaimantCity|ClaimantState|ClaimantZipCode|ClaimantDOB|Note|AdviceKey|AdviceFileName";

        Dictionary<string, XVar> _originalRecordsDictionary = new Dictionary<string, XVar>();
        public Dictionary<string, XVar> OriginalRecordsDictionary
            {
            get { return _originalRecordsDictionary; }
            set { _originalRecordsDictionary = value; }
            }


        private int _totalRecordsExported = 0;
        private string _wcediDSVFilepath = "";
        public int TotalRecordsExported
            {
            get { return _totalRecordsExported; }
            set { _totalRecordsExported = value; }
            }
        private int _totalRecordsReceivedToExport = 0;
        public int TotalRecordsReceivedToExport
            {
            get { return _totalRecordsReceivedToExport; }
            set { _totalRecordsReceivedToExport = value; }
            }

        private int _totalRecordsSentToOrbit = 0;
        public int TotalRecordsSendToOrbit
            {
            get { return _totalRecordsSentToOrbit; }
            set { _totalRecordsSentToOrbit = value; }
            }

        public System.Text.StringBuilder ErrorStringBuilder
            {
            get { return _errorStringBuilder; }
            set { _errorStringBuilder = value; }
            }
        // the side effect is to reset the string builder once this is used to print out messages.
        public System.Text.StringBuilder InfoStringBuilder
            {
            get { return _infoStringBuilder; }
            set { _infoStringBuilder = value; }
            }

        public string WCEDIDSVFilepath
            {
            get { return _wcediDSVFilepath; }
            set { _wcediDSVFilepath = value; }
            }

        private int _batch_id;
        public int Batch_Id
            {
            get { return _batch_id; }
            set { _batch_id = value; }
            }



        private WcediDSVFile wcediDSVFile;
        public WcediDSVFile WcediDSVFile
            {
            get { return wcediDSVFile; }
            set { wcediDSVFile = value; }
            }


        private WcediDSVFile financeDSVFile;

        public WcediDSVFile FinanceDSVFile
            {
            get { return financeDSVFile; }
            set { financeDSVFile = value; }
            }

        private string wcediVendorID;
        public string WcediVendorID
            {
            get { return wcediVendorID; }
            set { wcediVendorID = value; }
            }

        private string wcediVendorSiteCode;
        public string WcediVendorSiteCode
            {
            get { return wcediVendorSiteCode; }
            set { wcediVendorSiteCode = value; }
            }

        private RiskMasterOrbitPayments riskMasterOrbitPayments;

        public RiskMasterOrbitPayments RiskMasterOrbitPayments
            {
            get { return riskMasterOrbitPayments; }
            set { riskMasterOrbitPayments = value; }
            }

        private List<string> validWcediPageNameList = new List<string>();


        public List<string> ValidWcediPageNameList
            {
            get { return validWcediPageNameList; }
            set { validWcediPageNameList = value; }
            }


        private System.Configuration.Configuration config;
        public Configuration Config
            {
            get { return config; }
            set { config = value; }
            }


        public RiskMasterWCEDIPayments(int batch_id, System.Configuration.Configuration config = null)
            {
            Batch_Id = batch_id;

            if (config != null)
                {
                string wcediOutputDirectory = config.AppSettings.Settings["WCediDSVOutputDirectory"].Value;

                string wcediDSVFilepath = wcediOutputDirectory + Path.DirectorySeparatorChar + Batch_Id + "_wcedi.txt";

                WcediDSVFile = new WcediDSVFile(wcediDSVFilepath, wcediHeaderRow);


                string financeDSVFilepath = wcediOutputDirectory + Path.DirectorySeparatorChar + "wcedi_payments_" + Batch_Id + ".txt";
                string financeHeaderRow = string.Format("{0,-5}| {1,-10}| {2,-35}| {3,-45}| {4,-8}|{5,-7}|{6}{7}",
                    "WCEDI",
                    "TRANS DATE".Substring(0, 10),
                    "PARTIAL INVOICE NUMBER",
                    "PAYEE NAME",
                    "PAID",
                    "SUPPLIER",
                    "SITE",
                    System.Environment.NewLine);

                FinanceDSVFile = new WcediDSVFile(financeDSVFilepath, financeHeaderRow);

                WcediVendorID = config.AppSettings.Settings["wcediVendorID"].Value;

                WcediVendorSiteCode = config.AppSettings.Settings["wcediVendorSiteCode"].Value;
                }
            Config = config;
            TotalRecordsExported = 0;

            RiskMasterOrbitPayments = new RiskMasterOrbitPayments();


            }

        public void SendToWCEDIAndForOrbitApproval(string primePFDFilepath)
            {


            ProcessResult processResult = null;
            string wcediSplitPDFExecutable = config.AppSettings.Settings["WCediSplitPDFExecutable"].Value;
            if (!File.Exists(wcediSplitPDFExecutable))
                {
                throw new Exception("Could not find WcediSplitPDFExecutable " + wcediSplitPDFExecutable);
                }
            string wcediFTPExecutable = config.AppSettings.Settings["WCediFTPExecutable"].Value;
            if (!File.Exists(wcediFTPExecutable))
                {
                throw new Exception("Could not find WCediFTPExecutable " + wcediFTPExecutable);
                }
            if (PopulateWCEDIFile() > 0)
                {

                WriteWCEDIFileUpdateBatch();

                // Split Pages Into Pdf Files via executable

                string wcediPDFOutputDirectory = config.AppSettings.Settings["WCediPDFOutputDirectory"].Value;

                if (File.Exists(primePFDFilepath))
                    {
                    string wcediSplitPDFArguments = String.Format("-o \"{0}\" -f \"{1}\"", wcediPDFOutputDirectory, primePFDFilepath);

                    InfoStringBuilder.AppendLine(wcediSplitPDFArguments);
                    processResult = ProcessRunner.RunWinProcess(wcediSplitPDFExecutable, wcediSplitPDFArguments);
                    InfoStringBuilder.AppendLine(processResult.StandardOutput);

                    if (processResult.ExitCode != 0)
                        {
                        ErrorStringBuilder.AppendLine(processResult.StandardError);
                        throw new Exception("Split PDF into single files failed. Check Logs.");
                        }
                    }
                else
                    {
                    throw new Exception("Prime PDF file does not exist. Prime PDF location " + primePFDFilepath);
                    }

                // Move only files that correspond to the values in  into a directory that will be zipped by the FTP program 
                string wcediBatchOutputDirectory = config.AppSettings.Settings["WCediBatchPDFOutputDirectory"].Value;
                wcediBatchOutputDirectory = wcediBatchOutputDirectory + Path.DirectorySeparatorChar + Batch_Id;

                DirectoryInfo wcediBatchOutputDirectoryInfo = Directory.CreateDirectory(wcediBatchOutputDirectory);
                if (!wcediBatchOutputDirectoryInfo.Exists)
                    {
                    throw new Exception("Unable to create WCEDI output directory " + wcediBatchOutputDirectory);
                    }
                //               FileInfo fileInfo = new FileInfo("");
                foreach (String pageFileName in ValidWcediPageNameList)
                    {
                    string pagePDFFilepath = wcediPDFOutputDirectory + Path.DirectorySeparatorChar + pageFileName + ".pdf";
                    string wcediBatchOutputFilename = wcediBatchOutputDirectory + Path.DirectorySeparatorChar + pageFileName + ".pdf";
                    if (File.Exists(pagePDFFilepath))
                        {
                        File.Move(pagePDFFilepath, wcediBatchOutputFilename);
                        InfoStringBuilder.AppendLine("PDF file moved to : " + wcediBatchOutputFilename);
                        }
                    else
                        {
                        throw new Exception("PDF file does not exist : " + pagePDFFilepath);
                        }
                    }

                // Zip Files and FTP zip and WCEDI dsv file 

                string wcediFTPArguments = config.AppSettings.Settings["WCediFTPArguments"].Value + " -b " + Batch_Id;

                processResult = ProcessRunner.RunWinProcess(wcediFTPExecutable, wcediFTPArguments);
                InfoStringBuilder.AppendLine(processResult.StandardOutput);

                if (processResult.ExitCode != 0)
                    {
                    ErrorStringBuilder.AppendLine(processResult.StandardError);
                    throw new Exception("FTP process failed. Check Logs.");
                    }
                string generateFinancePDFExecutable = config.AppSettings.Settings["WCediFinanceGeneratePDFExecutable"].Value;
                string generateFinancePDFArguments = " -f " + FinanceDSVFile.WcediFilepath;
                processResult = ProcessRunner.RunWinProcess(generateFinancePDFExecutable, generateFinancePDFArguments);
                InfoStringBuilder.AppendLine(processResult.StandardOutput);

                if (processResult.ExitCode != 0)
                    {
                    ErrorStringBuilder.AppendLine(processResult.StandardError);
                    throw new Exception("Generate Finance PDF  process failed. Check Logs.");
                    }

                RiskMasterOrbitPayments.PopulateOrbitWithRiskmasterPayments();
                }
            else
                {
                ErrorStringBuilder.AppendLine("No Records to Send To WCEDI or Orbit");
                throw new Exception("SendToWCEDIAndForOrbitApproval - There are no validated records to approve!");

                }
            InfoStringBuilder.AppendLine("End  SendToWCEDIAndForOrbitApproval");
            }

        // confirm that the pd and pf records are valid records according to RiskMasterOrbitPayments



        public void Validate(List<String> selectedPFIDRecords)
            {
            InfoStringBuilder.AppendLine("RiskMasterWCEDIPayments Begin Validate");
            if (selectedPFIDRecords == null || selectedPFIDRecords.Count < 1)
                {
                return;
                }
            try
                {
                InfoStringBuilder.AppendLine("TotalRecordsReceivedToExport " + selectedPFIDRecords.Count);

                //List<String> formattedSelectRecordsList = selectedPFIDRecords.Select(x => string.Format("'{0}'", x)).ToList();


                // confirm all the selected records have Invoice Numbers that are not null or an empty space

                selectedPFIDRecords = ValidateInvoiceNumberNotNull(selectedPFIDRecords);

                InfoStringBuilder.AppendLine("TotalRecordsInvoiceNumberNotNull " + selectedPFIDRecords.Count);

                List<string> validPFIDList = RiskMasterOrbitPayments.Validate(selectedPFIDRecords);
                InfoStringBuilder.Append(RiskMasterOrbitPayments.InfoStringBuilder);
                ErrorStringBuilder.Append(RiskMasterOrbitPayments.ErrorStringBuilder);
                // make certain that hte Invoice Number is set, if null then invalid

                // another spot where a record may be invalidated is when matching to 
                // List<String> formattedSelectRecordsList = validPDIDList.Select(x => string.Format("{0}", x)).ToList();
                if (validPFIDList.Count() > 0)
                    {
                    InfoStringBuilder.AppendLine("TotalValidRecords " + validPFIDList.Count());
                    String inSelectRecordsClause = String.Join(",", validPFIDList);

                    
                    String updatePfIsEDIPayment = string.Format(formatUpdatePfIsEDIPaymentString, WcediVendorID, WcediVendorSiteCode, Batch_Id, inSelectRecordsClause);
                    InfoStringBuilder.AppendLine(updatePfIsEDIPayment);
                    oracleDBFacade.Exec(updatePfIsEDIPayment);
                    }
                else
                    {
                    InfoStringBuilder.AppendLine("NO VALID RECORDS!");
                    }

                }
            catch (Exception ex)
                {
                // ErrorStringBuilder.AppendFormat(ex.ToString() + "\n");
                ErrorStringBuilder.AppendFormat("A ({0}) exception  occurred.\n",
                                  ex.GetType().Name);
                ErrorStringBuilder.AppendFormat("   Message: {0}\n", ex.Message);
                ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", ex.StackTrace);
                Exception ie = ex.InnerException;
                if (ie != null)
                    {
                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                    ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }
                }

            InfoStringBuilder.AppendLine("End Validate of RiskMasterWCEDIPayments");
            }

        private List<string> ValidateInvoiceNumberNotNull(List<string> selectedPFIDRecords)
            {
            List<string> validRecordsWithInvoiceNumbers = new List<string>();
            String inSelectRecordsClause = String.Join(",", selectedPFIDRecords);
            String selectInvalidEDIInvoiceNumberFormatted = string.Format(selectEDIInvoiceNumberString, inSelectRecordsClause);
            QueryResult dynamicInvalidEDIInvoiceNumberResult = DB.Query(selectInvalidEDIInvoiceNumberFormatted);
            if (dynamicInvalidEDIInvoiceNumberResult != null)
                {

                XVar record = dynamicInvalidEDIInvoiceNumberResult.fetchAssoc();
                while (record != null && record.Count() > 0)
                    {
                    if (!String.IsNullOrEmpty(record["PF_ID"]) && !String.IsNullOrEmpty(record["INVOICE_NUMBER"]))
                        {
                        OriginalRecordsDictionary.Add(record["PD_ID"], record);
                        validRecordsWithInvoiceNumbers.Add(record["PF_ID"]);
                        record = dynamicInvalidEDIInvoiceNumberResult.fetchAssoc();

                        }
                    else
                        {
                        throw new NullReferenceException("INVOICE_NUMBER is null for the record: [" + record["PF_ID"].ToString() + "]");
                        }

                    }
                }
            return validRecordsWithInvoiceNumbers;
            }

        private int PopulateWCEDIFile()
            {

            int validRowCount = 0;
            try
                {

                String whereWCEDIInClause = string.Format(formatWCEDIPaymentsWhereString, Batch_Id);

                dynamic dynamicWCEDIResult = oracleDBFacade.Select("XXCOK.XXCOK_RM_WCEDI_DSV", whereWCEDIInClause);
                if (dynamicWCEDIResult != null)
                    {


                    XVar record = null;
                    do
                        {
                        record = dynamicWCEDIResult.fetchAssoc();
                        if (record == null || record.Count() < 1)
                            {
                            continue;
                            }
                        else
                            {

                            XVar bill_id = record["INVOICE_NUMBER"];
  //                          InfoStringBuilder.AppendLine("INVOICE_NUMBER " + bill_id.ToString());
                            if (bill_id.IsString() && !string.IsNullOrEmpty(bill_id.ToString()))
                                {

                                string DSVRow = record["CSV_ROW"].ToString();
                                if (!String.IsNullOrEmpty(DSVRow))
                                    {
                                    validRowCount++;
         
                                   // InfoStringBuilder.AppendLine(DSVRow);
                                    WcediDSVFile.Add(DSVRow);
                                    ValidWcediPageNameList.Add(bill_id.ToString());
                                    XVar originalRecord = new XVar();
                                    //InfoStringBuilder.AppendFormat("PD ID is {0}{1}", record["PD_ID"], System.Environment.NewLine);
                                    if (OriginalRecordsDictionary.TryGetValue(record["PD_ID"], out originalRecord))
                                        {


                                        FinanceDSVFile.Add(
                                           string.Format("{0,-5}| {1,-10}| {2,-35}| {3,-45}| {4,-8}| {5,-6} |{6}",
                                            WcediVendorID,
                                            originalRecord["TRANSACTION_DATE"].Substring(0, 10),
                                            string.Format("{0} - {1}", originalRecord["CHECK_NUMBER"], originalRecord["INVOICE_NUMBER"]),
                                            originalRecord["PAYEE_NAME"],
                                            originalRecord["SPLIT_AMOUNT"],
                                            originalRecord["NPI_NUMBER"],
                                            originalRecord["TITLE"]))  ;
                                        }
                                    else
                                        {
                                        InfoStringBuilder.AppendFormat("unable to find original Record for PD ID is {0}{1}", record["PD_ID"], System.Environment.NewLine);
                                        } 
                                    }
                                else
                                    {
                                    ErrorStringBuilder.AppendFormat("In PopulateWCEDIFile DSV_ROW is null or is empty pd_id: {0} import_id {1} import_date {2} payment_id {3}{4}{5}{6}", record["PD_ID"], record["IMPORT_ID"], record["IMPORT_DATE"], record["PAYMENT_ID"], System.Environment.NewLine, record["CSV_ROW"], System.Environment.NewLine);
                                    }
                                }
                            else
                                {
                                ErrorStringBuilder.AppendFormat("In PopulateWCEDIFile INVOICE_NUMBER not a string or is empty pd_id: {0} import_id {1} import_date {2} payment_id {3}{4}", record["PD_ID"], record["IMPORT_ID"], record["IMPORT_DATE"], record["PAYMENT_ID"], System.Environment.NewLine);
                                }
                            }
                        } while ((record != null));
                    InfoStringBuilder.AppendLine("Number of records to send to WCEDI " + validRowCount);
                    }
                }
            catch (Exception ex)
                {
                validRowCount = 0;
                ErrorStringBuilder.AppendFormat(ex.ToString() + "\n");
                ErrorStringBuilder.AppendFormat("A {0} occurred.\n",
                                  ex.GetType().Name);
                ErrorStringBuilder.AppendFormat("   Message: {0}\n", ex.Message);
                ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", ex.StackTrace);
                Exception ie = ex.InnerException;
                if (ie != null)
                    {
                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                    ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }
                }
            return validRowCount;
            }
        private void WriteWCEDIFileUpdateBatch()
            {
            try
                {
                if (wcediDSVFile.Count() > 0)
                    {

                    wcediDSVFile.WriteToFile();
                    FinanceDSVFile.WriteToFile();
                    // total up the number and payments


                    string updateBatchTotalFormat =
                       @"Update xxcok.xxcok_rm_batch 
                             set record_count = {0},
                             PAYMENT_TOTAL = (select sum(pd.split_amount)
                             from xxcok.xxcok_rm_import_pf pf inner join xxcok.xxcok_rm_import_pd pd on
                             pf.import_id = pd.import_id and pf.import_date = pd.import_date and 
                             pf.payment_id = pd.payment_id where pf.BATCH_ID = {1} AND pf.IS_EDI_PAYMENT = 1 AND pf.IS_VALID = 1 AND pf.IS_PROCESSED = 0)
                             where BATCH_ID = {1}";

                    string updateBatchTotalString = String.Format(updateBatchTotalFormat, wcediDSVFile.Count(), Batch_Id);
                    oracleDBFacade.Exec(updateBatchTotalString);

                    }
                else
                    {
                    InfoStringBuilder.AppendLine("No records to update batch " + Batch_Id);
                    }
                }
            catch (Exception ex)
                {
                ErrorStringBuilder.AppendFormat(ex.ToString() + "\n");
                ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n",
                                  ex.GetType().Name);
                ErrorStringBuilder.AppendFormat("   Message: {0}\n", ex.Message);
                ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", ex.StackTrace);
                Exception ie = ex.InnerException;
                if (ie != null)
                    {
                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                    ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }
                }
            }
        }

    }

