using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
namespace runnerDotNet
    {
    /*


    Check for Validity
    a)	NPI_Number  is a Required Field in RISKMASTER_PF_IMPORT table. Error shows  "Supplier NBR is missing in Entity NPI_NUMBER".
    b)	 An NPI_Number  must exist in XXCOK_AP_SUPPLIERS_VW. Error shows "Supplier NBR is invalid in Entity NPI_NUMBER"
    c)	Title is a Required Field in RISKMASTER_PF_IMPORT table. Error shows "Site Codes is missing in Entity Title"
    d)	Pay Site record must exist on  AP_SUPPLIER_SITES_ALL for NPI_Number as Vendor_ID = NPI_Number and Pay_Site_Flag = ‘Y’ and VENDOR_SITE_CODE = upper(Title). Error shows "Supplier Site Code is invalid in Entity Title"

    Throw Exceptions when something particularly offensive happend, 
    otherwise log failures and go on.  Invalidated records whill not be included in the population routine


     */

    public class RiskMasterOrbitPayments
        {
        
        OracleDBFacade oracleDBFacade = new OracleDBFacade();
        IList<XVar> insertErrorList = new List<XVar>();
        private System.Text.StringBuilder _errorStringBuilder = new System.Text.StringBuilder();
        private System.Text.StringBuilder _infoStringBuilder = new System.Text.StringBuilder();
        private static string formatPfImportToProcessWhereString = @" IS_PROCESSED = 0 AND IS_VALID = 0  AND PF_ID in ({0})";

        private int _totalRecordsValidated = 0;
        public int TotalRecordsValidated
            {
            get { return _totalRecordsValidated; }
            set { _totalRecordsValidated = value; }
            }
        private int _totalRecordsReceivedToValidate = 0;
        public int TotalRecordsReceivedToValidate
            {
            get { return _totalRecordsReceivedToValidate; }
            set { _totalRecordsReceivedToValidate = value; }
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
        public System.Text.StringBuilder InfoStringBuilder
            {
            get { return _infoStringBuilder; }
            set { _infoStringBuilder = value; }
            }
        private List<string> _infoValidPFID = new List<string>();
        public List<string> ValidPFIDList
            {
            get { return _infoValidPFID; }
            set { _infoValidPFID = value; }
            }
        public RiskMasterOrbitPayments()
            {
            TotalRecordsValidated = 0;
            }
        public void SendForApproval (List<String> selectedRecords)
            {
            Validate(selectedRecords);

            PopulateOrbitWithRiskmasterPayments();

            }

        public List<string> Validate(List<String> selectedRecords)
            {
            InfoStringBuilder.AppendLine("RiskMasterOrbitPayments Begin Validate");
            if (selectedRecords != null)
                {
                TotalRecordsReceivedToValidate = selectedRecords.Count;
                }
            else
                {
                InfoStringBuilder.AppendLine("No records provided for Validation");
                return ValidPFIDList;
                }
            if (TotalRecordsReceivedToValidate > 0)
                {
                try
                    {
                    InfoStringBuilder.AppendLine("Records to validate " + TotalRecordsReceivedToValidate);
                    QueryResult dynamicPfImportResult = GetPFRecordsToValidateFromList(selectedRecords);
                    if (dynamicPfImportResult != null)
                        {

                        ValidationRecord validationRecord = new ValidationRecord(dynamicPfImportResult.fetchAssoc());
                        while ( validationRecord.record != null && validationRecord.record["PF_ID"] != null)
                            {
                            string pf_id = validationRecord.record["PF_ID"];

                            try
                                {

                                XVar recordWhereClause = OracleDBFacade.buildPfImportRecordPrimaryKeyClause(validationRecord.record);

                                if (recordWhereClause != null)
                                    {
                                    bool valid = this.validateCokRiskmasterPfImportRecord(validationRecord);
                                    if (valid)
                                        {
                                        this.updateCokRiskmasterPfImportRecord(recordWhereClause, "1");

                                        ValidPFIDList.Add(validationRecord.record["PF_ID"]);
                                        TotalRecordsValidated = TotalRecordsValidated + 1;
                                        InfoStringBuilder.AppendFormat(@"Valid: PF_ID {0} for Orbit{1}", pf_id, System.Environment.NewLine);
                                        }
                                    else
                                        {
                                        ErrorStringBuilder.AppendFormat(@"Invalid! PF_ID {0} for Orbit{1}", pf_id, System.Environment.NewLine);
                                        this.updateCokRiskmasterPfImportRecord(recordWhereClause, "0");

                                        buildRiskMasterPaymentErrorList(validationRecord);
                                        }
                                    }
                                else
                                    {
                                    validationRecord.message = " Record Where Clause is NULL. This should never happen!";
                                    buildRiskMasterPaymentErrorList(validationRecord);
                                    }

                                }
                            catch (Exception validateException)
                                {



                                ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n",
                                                  validateException.GetType().Name);
                                ErrorStringBuilder.AppendFormat("   Message: {0}\n", validateException.Message);
                                ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", validateException.StackTrace);
                                Exception ie = validateException.InnerException;
                                if (ie != null)
                                    {
                                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                                    ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                                    }

                                }
                            validationRecord = new ValidationRecord(dynamicPfImportResult.fetchAssoc());
                            }
                        }
                    else
                        {
                        InfoStringBuilder.AppendLine("No records to validate returned from Select");
                        }


                    }
                catch (Exception validateException)
                    {



                    ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n",
                                      validateException.GetType().Name);
                    ErrorStringBuilder.AppendFormat("   Message: {0}\n", validateException.Message);
                    ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", validateException.StackTrace);
                    Exception ie = validateException.InnerException;
                    if (ie != null)
                        {
                        ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                        ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                        ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                        ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                        }
                    throw validateException;
                    }
                }
            else
                {
                InfoStringBuilder.AppendLine("No records provided for Validation");
                }
            return ValidPFIDList;
            }
        public dynamic GetPFRecordsToValidateFromList(List<string> selectedRecords)
            {

            List<String> formattedSelectRecordsList = selectedRecords.Select(x => string.Format("{0}", x)).ToList();

            String inSelectRecordsClause = String.Join(",", formattedSelectRecordsList);

            String pfImportWhereToProcessInClause = string.Format(formatPfImportToProcessWhereString, inSelectRecordsClause);
            return oracleDBFacade.Select("xxcok.xxcok_rm_import_pf  PF", pfImportWhereToProcessInClause);
            }

        public void PopulateOrbitWithRiskmasterPayments() 
            {
            
            try
                {
                InfoStringBuilder.AppendFormat(@"In PopulateOrbitWithRiskmasterPayments {0} valid record{1}{2}", TotalRecordsValidated, TotalRecordsValidated > 1 ? "s" : "", System.Environment.NewLine);
                this.PopulateOrbitWithRiskmasterPaymentsTable();

                this.updateRiskMasterPFImportTables();
                }
            catch (Exception validateException)
                {

                ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n",
                                  validateException.GetType().Name);
                ErrorStringBuilder.AppendFormat("   Message: {0}\n", validateException.Message);
                ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", validateException.StackTrace);
                Exception ie = validateException.InnerException;
                if (ie != null)
                    {
                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                    ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }
                throw validateException;
                }
            }


        private bool validateCokRiskmasterPfImportRecord(ValidationRecord validationRecord)
            {
            dynamic record = validationRecord.record;
            string npiNumber = null;
            string title = null;
            if (!this.hasValidNpiNumber(record, ref npiNumber))
                {
                // insert the error string that is now held by npiValidationString

                validationRecord.message = "Supplier NBR is missing in Entity NPI_NUMBER";
                return false;
                }
            if (!this.hasValidTitle(record, ref title))
                {
                // insert the error string that is now held by npiValidationString

                validationRecord.message = "Site Codes is missing in Entity Title";
                return false;
                }
            //An NPI_Number  must exist in XXCOK_AP_SUPPLIERS_VW. 
            //Error shows "Supplier NBR is invalid in Entity NPI_NUMBER"
            if (!this.validateSupplierNbr(npiNumber))
                {
                validationRecord.message = "Supplier NBR is invalid in Entity NPI_NUMBER";
                return false;
                }
            //Pay Site record must exist on  AP_SUPPLIER_SITES_ALL for NPI_Number as Vendor_ID = NPI_Number and Pay_Site_Flag = ‘Y’ and VENDOR_SITE_CODE = upper(Title). 
            //Error shows "Supplier Site Code is invalid in Entity Title"
            if (!this.validateSupplierSiteCode(npiNumber, title))
                {

                validationRecord.message = "Supplier Site Code is invalid in Entity Title";
                return false;
                }
            XVar validRow = OracleDBFacade.buildPfImportRecordPrimaryKeyClause(record);
            String validMessage = string.Format("Valid Record:{0}", validRow.ToString());
            validationRecord.message = validMessage;
            return true;
            }
        private void updateCokRiskmasterPfImportRecord(XVar recordWhereClause, string isValid)
            {
            XVar setIsValidColumn = new XVar();
            setIsValidColumn.SetArrayItem("IS_VALID", isValid);
            XVar userName = Security.getUserName();
          
            setIsValidColumn.SetArrayItem("UPDATED_BY_USER", userName);

            oracleDBFacade.Update("xxcok.xxcok_rm_import_pf", setIsValidColumn, recordWhereClause);

            }
        private void buildRiskMasterPaymentErrorList(ValidationRecord validationRecord)
            {
            XVar insertErrorRow = OracleDBFacade.buildPfImportRecordPrimaryKeyClause(validationRecord.record);
            string errorMessage = string.Format("Error: %1", validationRecord.message);
            insertErrorRow.SetArrayItem("ERROR_DESCR", errorMessage);
            insertErrorList.Add(insertErrorRow);
            }
        private bool hasValidNpiNumber(dynamic record, ref string npiValidationString)
            {

            XVar value = record["NPI_NUMBER"];

            if (value != null && (value.IsString()) && (value.ToString().Length > 0))
                {
                npiValidationString = value;
                return true;
                }

            return false;
            }
        private bool hasValidTitle(dynamic record, ref string titleValidationString)
            {
            XVar value = record["TITLE"];

            if ((value != null) && (value.IsString()) && (value.ToString().Length > 0))
                {
                titleValidationString = value;
                return true;
                }

            return false;
            }
        private bool validateSupplierSiteCode(string npiNumber, string title)
            {
            bool returnSuccess = false;

            try
                {
                string formatSupplierSiteCodeStr = @"SELECT APSSA.VENDOR_ID AS VENDOR_ID
                                                    FROM AP.AP_SUPPLIER_SITES_ALL APSSA
                                                    INNER JOIN AP.AP_SUPPLIERS APS
                                                    ON APSSA.VENDOR_ID = APS.VENDOR_ID
                                                    WHERE APS.SEGMENT1 = {0}
                                                    AND UPPER(APSSA.VENDOR_SITE_CODE) = UPPER('{1}')
                                                    AND APSSA.PAY_SITE_FLAG IS NOT NULL 
                                                    AND APSSA.PAY_SITE_FLAG = 'Y'";
                string validateSupplierSiteCodeStmt = String.Format(formatSupplierSiteCodeStr, npiNumber, title);

                dynamic qresult = oracleDBFacade.Query(validateSupplierSiteCodeStmt);

                XVar recordXVar = qresult.fetchAssoc();

                if (recordXVar != null && recordXVar.Count() > 0)
                    {
                    dynamic record = recordXVar.Value;
                    XVar value = record["VENDOR_ID"];

                    if (value != null)
                        {
                        returnSuccess = true;
                        }

                    }

                }
            catch (Exception e)
                {


                ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n",
                                    e.GetType().Name);
                ErrorStringBuilder.AppendFormat("   Message: {0}\n", e.Message);
                ErrorStringBuilder.AppendFormat("   Stack Trace:   {0}\n", e.StackTrace);
                Exception ie = e.InnerException;
                if (ie != null)
                    {
                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                    ErrorStringBuilder.AppendFormat("      Stack Trace:   {0}\n", ie.StackTrace);
                    }

                }
            return returnSuccess;
            }
        private bool validateSupplierNbr(string npiNumber)
            {
            bool returnSuccess = false;
            try
                {

                XVar testSelectDictionary = new XVar();
                string formatSupplierNbrStr = @"SELECT APSSA.VENDOR_ID AS VENDOR_ID
                                                    FROM AP.AP_SUPPLIER_SITES_ALL APSSA
                                                    INNER JOIN AP.AP_SUPPLIERS APS
                                                    ON APSSA.VENDOR_ID = APS.VENDOR_ID
                                                    WHERE APS.SEGMENT1 = {0}";
                string validateSupplierNbrStmt = String.Format(formatSupplierNbrStr, npiNumber);

                dynamic qresult = oracleDBFacade.Query(validateSupplierNbrStmt);

                XVar recordXVar = qresult.fetchAssoc();

                if (recordXVar != null && recordXVar.Count() > 0)
                    {
                    dynamic record = recordXVar.Value;
                    XVar value = record["VENDOR_ID"];

                    if (value != null)
                        {
                        returnSuccess = true;
                        }

                    }

                }
            catch (Exception e)
                {

                ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n",
                                    e.GetType().Name);
                ErrorStringBuilder.AppendFormat("   Message: {0}\n", e.Message);
                ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", e.StackTrace);
                Exception ie = e.InnerException;
                if (ie != null)
                    {
                    ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                    ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                    ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                    ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                    }

                }
            return returnSuccess;
            }

        private void PopulateOrbitWithRiskmasterPaymentsTable()
            {
            string insertStatement = @"INSERT INTO XXCOK.XXCOK_RISK_MASTER_PAYMENTS
                                    (CTL_NUMBER,
                                    CLAIM_ID,
                                    CLAIM_NUMBER,
                                    NAME,
                                    ADDR1,
                                    ADDR2,
                                    CITY,
                                    STATE,
                                    ZIP_CODE,
                                    AMOUNT,
                                    TRANS_DATE,
                                    INVOICE_NUMBER,
                                    DEPT,
                                    LINE_OF_BUS_CODE,
                                    SUPPLIER_NBR,
                                    VENDOR_SITE_CODE,
                                    INVOICE_TOTAL,
                                    TRANS_NUMBER)
                                    SELECT 
                                    pf.CONTROL_NUMBER,
                                    pf.CLAIM_ID,
                                    pf.CLAIM_NUMBER,
                                    pf.PAYEE_NAME,
                                    pf.STREET_ADDRESS_1,
                                    pf.STREET_ADDRESS_2,
                                    pf.CITY,
                                    pf.STATE,
                                    pf.ZIP_CODE,
                                    pd.SPLIT_AMOUNT,
                                    pf.TRANSACTION_DATE,
                                    pd.INVOICE_NUMBER,
                                    CASE WHEN SUBSTR(CLAIM_NUMBER, 3, 1) = 'I' THEN SUBSTR(CLAIM_NUMBER,4,5) ELSE SUBSTR(CLAIM_NUMBER,3,5) END AS DEPT,
                                    CASE WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'WC' THEN 243
                                    WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'VA' THEN 242 
                                    WHEN SUBSTR(CLAIM_NUMBER, 1, 2) = 'GC' THEN 241 
                                    ELSE NULL END AS LINE_OF_BUS_CODE,
                                    pf.NPI_NUMBER,
                                    pf.TITLE,
                                    0,
                                    pf.CHECK_NUMBER
                                    FROM XXCOK.XXCOK_RM_IMPORT_PF PF 
                                    INNER JOIN XXCOK.XXCOK_RM_IMPORT_PD PD
                                        on PF.IMPORT_ID = PD.IMPORT_ID and
                                        PF.IMPORT_DATE = PD.IMPORT_DATE and
                                        PF.PAYMENT_ID = PD.PAYMENT_ID
                                    where PF.is_valid = 1
                                    AND PF.IS_PROCESSED = 0";
 
 
            oracleDBFacade.Exec(insertStatement);

            }
        private void updateRiskMasterPFImportTables()
            {
            XVar userName = Security.getUserName();

            string updateStatementFormat = @"UPDATE XXCOK.XXCOK_RM_IMPORT_PF PF 
                                        SET  IS_PROCESSED = 1 ,
                                            UPDATED_BY_USER = '{0}' 
                                        WHERE IS_VALID = 1 AND IS_PROCESSED = 0
                                        AND EXISTS (
                                        SELECT 1 FROM XXCOK.XXCOK_RISK_MASTER_PAYMENTS RMP
                                        WHERE PF.CONTROL_NUMBER = RMP.CTL_NUMBER  AND
                                        PF.CLAIM_ID = RMP.CLAIM_ID AND
                                        PF.CLAIM_NUMBER = RMP.CLAIM_NUMBER)";

            string updateStatement = String.Format(updateStatementFormat, userName.ToString());
            oracleDBFacade.Exec(updateStatement);

            }
        private static bool isNumericType(object o)
            {
            switch (Type.GetTypeCode(o.GetType()))
                {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
                }
            }
        }
    }
