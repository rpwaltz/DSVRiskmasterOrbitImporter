using System;
using System.IO;
using System.Configuration;
using System.Collections.Generic;
using System.Text;

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
    public class RiskMasterDSVParser
        {
        private static string _pf_import_id;
        private static string _pd_import_id;
        private static string _import_date;
        private static string[] _pfDSVColumnNames = new string[] {
            "PAYMENT_ID",
            "CONTROL_NUMBER",
            "CHECK_NUMBER",
            "PAYEE_NAME",
            "STREET_ADDRESS_1",
            "STREET_ADDRESS_2",
            "CITY",
            "STATE",
            "ZIP_CODE",
            "CHECK_MEMO",
            "CLAIM_NUMBER",
            "CLAIMANT_NAME",
            "CHECK_AMOUNT",
            "CHECK_DATE",
            "TRANSACTION_DATE",
            "STATUS_CODE",
            "PAYMENT_FLAG",
            "CLEARED_FLAG",
            "CLAIM_ID",
            "INVOICE_DATE",
            "CLAIMANT_FIRST_NAME",
            "CLAIMANT_LAST_NAME",
            "CLAIMANT_ABBREVIATION",
            "NPI_NUMBER",
            "TITLE",
            "TIN",
            "UPDATED_BY_USER",
            "ADDED_BY_USER",
        };

        private static string[] _pdDSVColumnNames = new string[] {
            "PAYMENT_ID",
            "PAYMENT_DETAIL_ID",
            "TRANSACTION_TYPE",
            "SPLIT_AMOUNT",
            "GENERAL_LEDGER_ACCOUNT",
            "FROM_DATE",
            "TO_DATE",
            "INVOICED_BY",
            "INVOICE_AMOUNT",
            "INVOICE_NUMBER",
            "PO_NUMBER"};

        private char _DSVDelimiter;
        private char _DSVQualifier;

        public RiskMasterDSVParser(char DSVDelimiter, char DSVQualifier)
            {
            _DSVDelimiter = DSVDelimiter;
            _DSVQualifier = DSVQualifier;
            RiskMasterDSVParser._import_date = DateTime.Today.ToString("yyyyMMdd");

            }
        /**
		 * prepare a datastructure that can be used to insert rows into the xxcok_rm_pd_import database table
		 */
        public IList<XVar> getPfDSVContents(string pfDSVFile)
            {
            IList<XVar> pfDSVContentsArrayList = new List<XVar>();
            // open the pf DSV file,  parse all the contents and produce data structure
            RiskMasterDSVParser._pf_import_id = getFormatIdFromFilePath(pfDSVFile);
            string fileContents = File.ReadAllText(pfDSVFile);
            IList<string> allDSVRows = this.parseFileIntoLines(fileContents);
            if (allDSVRows.Count > 0)
                {
                foreach (string DSVRow in allDSVRows)
                    {
                    if (DSVRow.Length > 1)
                        {
                        try
                            {
                            IList<String> DSVRowList = this.parseDSVRowtoList(DSVRow);

                            XVar DSVRowDictionary = this.parsePfDSVRowListToDictionary(DSVRowList);
                            pfDSVContentsArrayList.Add(DSVRowDictionary);
                            }
                        catch (Exception ex)
                            {
                            RiskMasterImporter.ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n", ex.GetType().Name);
                            RiskMasterImporter.ErrorStringBuilder.AppendFormat("   Message: {0}\n", ex.Message);
                            RiskMasterImporter.ErrorStringBuilder.AppendFormat("   Stack Trace:\n   {0}\n", ex.StackTrace);
                            Exception ie = ex.InnerException;
                            if (ie != null)
                                {
                                RiskMasterImporter.ErrorStringBuilder.AppendFormat("   The Inner Exception:\n");
                                RiskMasterImporter.ErrorStringBuilder.AppendFormat("      Exception Name: {0}\n", ie.GetType().Name);
                                RiskMasterImporter.ErrorStringBuilder.AppendFormat("      Message: {0}\n", ie.Message);
                                RiskMasterImporter.ErrorStringBuilder.AppendFormat("      Stack Trace:\n   {0}\n", ie.StackTrace);
                                }
                            }
                        }
                    }
                }
            else
                {
                throw new Exception("Pf DSV file is empty");
                }
            return pfDSVContentsArrayList;
            }
        /**
		 * prepare a datastructure that can be used to insert rows into the xxcok_rm_pd_import database table
		 */
        public IList<XVar> getPdDSVContents(string pdDSVFile)
            {
            IList<XVar> pdDSVContentsArrayList = new List<XVar>();
            // open the pf DSV file,  parse all the contents and produce data structure
            RiskMasterDSVParser._pd_import_id = getFormatIdFromFilePath(pdDSVFile);
            string fileContents = File.ReadAllText(pdDSVFile);
            IList<string> allDSVRows = this.parseFileIntoLines(fileContents);
            if (allDSVRows.Count > 0)
                {
                foreach (string DSVRow in allDSVRows)
                    {
                    if (DSVRow.Length > 1)
                        {
                        IList<String> DSVRowList = this.parseDSVRowtoList(DSVRow);

                        XVar DSVRowDictionary = this.parsePdDSVRowListToDictionary(DSVRowList);
                        pdDSVContentsArrayList.Add(DSVRowDictionary);
                        }
                    }
                }
            else
                {
                throw new Exception("Pd DSV file is empty");
                }
            return pdDSVContentsArrayList;
            }
        private XVar parsePfDSVRowListToDictionary(IList<string> DSVRowList)
            {
            XVar pfDSVRowDictionary = new XVar();
            pfDSVRowDictionary.SetArrayItem("IMPORT_ID", RiskMasterDSVParser._pf_import_id);
            pfDSVRowDictionary.SetArrayItem("IMPORT_DATE", RiskMasterDSVParser._import_date);
            pfDSVRowDictionary.SetArrayItem("IS_VALID", "0");
            pfDSVRowDictionary.SetArrayItem("IS_PROCESSED", "0");
            // row delimiter passed in?
            if (DSVRowList.Count < 25)
                {
                throw new Exception("Pf DSV file should have 25 columns. Unable to discover 25 columns");
                }

            for (int rowNum = 0; rowNum < DSVRowList.Count; ++rowNum)
                {
                pfDSVRowDictionary.SetArrayItem(_pfDSVColumnNames[rowNum], DSVRowList[rowNum]);

                }
            
            pfDSVRowDictionary.SetArrayItem("ORIG_SUPPLIER_NUMBER", pfDSVRowDictionary.GetArrayItem("NPI_NUMBER"));
            //pfDSVRowDictionary.Add
            return pfDSVRowDictionary;
            }

        private XVar parsePdDSVRowListToDictionary(IList<string> DSVRowList)
            {
            XVar pdDSVRowDictionary = new XVar();
            pdDSVRowDictionary.SetArrayItem("IMPORT_ID", RiskMasterDSVParser._pd_import_id);
            pdDSVRowDictionary.SetArrayItem("IMPORT_DATE", RiskMasterDSVParser._import_date);
            pdDSVRowDictionary.SetArrayItem("IS_VALID", "0");
            pdDSVRowDictionary.SetArrayItem("IS_PROCESSED", "0");
            // row delimiter passed in?
            if (DSVRowList.Count < 11)
                {
                throw new Exception("Pd DSV file should have 11 columns. Unable to discover 11 columns");
                }

            for (int rowNum = 0; rowNum < 11; ++rowNum)
                {
                pdDSVRowDictionary.SetArrayItem(_pdDSVColumnNames[rowNum], DSVRowList[rowNum]);

                }
            // After WCEDI is processed the original INVOICE_NUMBER is replaced with the identifier for WCEDI

            //pfDSVRowDictionary.Add
            return pdDSVRowDictionary;
            }
        private IList<String> parseDSVRowtoList(string DSVRow)
            {

            IList<string> listOfValues = new List<string>();
            char[] charTextArray = DSVRow.ToCharArray();
            bool startOfValue = true;
            bool endOfValue = false;
            bool parseString = false;
            char lastChar = '\n';
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < charTextArray.Length; i++)
                {
                if (charTextArray[i].Equals(this._DSVDelimiter))
                    {
                    if (parseString)
                        {
                        // the string is being parsed, the delimiter is considered part of the value
                        stringBuilder.Append(charTextArray[i]);
                        }
                    else if (startOfValue)
                        {
                        // this condition handles an empty value
                        endOfValue = true;
                        }
                    else if (stringBuilder.Length > 0)
                        {
                        // this condition represents a value that has ended, and a new one will begin
                        endOfValue = true;
                        startOfValue = true;
                        }
                    else
                        {
                        // this condition represents the start of a new value
                        startOfValue = true;
                        }
                    }
                else if (charTextArray[i].Equals(this._DSVQualifier))
                    {
                    if (startOfValue)
                        {
                        startOfValue = false;
                        parseString = true;
                        }
                    else if (parseString)
                        {
                        parseString = false;
                        endOfValue = true;
                        }
                    else
                        {
                        parseString = true;
                        stringBuilder.Append(charTextArray[i]);
                        }
                    }
                else
                    {
                    startOfValue = false;
                    stringBuilder.Append(charTextArray[i]);
                    }

                lastChar = charTextArray[i];
                if (endOfValue)
                    {
                    string valueOfField = stringBuilder.ToString();
                    listOfValues.Add(valueOfField.Trim());
                    stringBuilder = new StringBuilder();
                    endOfValue = false;
                    parseString = false;
                    }

                }
            if (stringBuilder.Length > 0)
                {
                string valueOfField = stringBuilder.ToString();
                listOfValues.Add(valueOfField.Trim());
                }
            else if (lastChar == this._DSVDelimiter)
                {
                listOfValues.Add("");
                }


            return listOfValues;
            }
        private IList<string> parseFileIntoLines(string fileContent)
            {
            IList<string> allDSVRows = new List<string>();

            char[] charTextArray = fileContent.ToCharArray();
            bool startOfValue = true;
            bool endOfValue = false;
            bool parseString = false;
            bool endOfRow = false;
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < charTextArray.Length; i++)
                {
                if (charTextArray[i].Equals(this._DSVDelimiter))
                    {
                    // the delimiter represents a break between two values. 
                    stringBuilder.Append(charTextArray[i]);

                    if (startOfValue)
                        {
                        // this condition handles an empty value
                        endOfValue = true;
                        }
                    else if (stringBuilder.Length > 0)
                        {
                        // this condition represents a value that has ended, and a new one will begin
                        endOfValue = true;
                        startOfValue = true;
                        }
                    }
                else if (charTextArray[i].Equals(this._DSVQualifier))
                    {
                    // the qualifier represents a value that is a string. numbers do not need qualifiers inside a value
                    stringBuilder.Append(charTextArray[i]);
                    if (startOfValue)
                        {
                        startOfValue = false;
                        parseString = true;
                        }
                    else if (parseString)
                        {
                        parseString = false;
                        endOfValue = true;
                        }
                    else
                        {
                        parseString = true;
                        }
                    }
                else if (charTextArray[i].Equals('\n') || charTextArray[i].Equals('\r'))
                    {
                    if (parseString)
                        {
                        stringBuilder.Append(charTextArray[i]);
                        }
                    else if (endOfValue)
                        {
                        // will trigger pushing the row onto the lines list
                        endOfRow = true;
                        }
                    }
                else
                    {
                    startOfValue = false;
                    stringBuilder.Append(charTextArray[i]);
                    }

                if (endOfRow && (stringBuilder.Length > 0))
                    {

                    string valueOfField = stringBuilder.ToString();

                    allDSVRows.Add(valueOfField.Trim());
                    stringBuilder = new StringBuilder();
                    endOfValue = false;
                    parseString = false;
                    endOfRow = false;
                    }

                }

            return allDSVRows;
            }
        private string getFormatIdFromFilePath(string filePath)
            {
            string fileName = Path.GetFileName(filePath);
            int fileExtPosition = fileName.LastIndexOf('.');
            string fileNameNoFileExt = fileName.Remove(fileExtPosition, (fileName.Length - fileExtPosition));
            string importID = fileNameNoFileExt.Substring(2);
            return importID;
            }
        }
    }
