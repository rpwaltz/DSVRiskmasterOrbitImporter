using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

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
    /// <summary>
    //  Searches through a directory to discover PD and PF files that have been exported by RiskMaster.
    //  For each PD and PF file found, the DSV files will be parsed into a Collection datastructure
    //  The parsed DSV datastructure will be uploaded to the ORBIT Import Routines tables
    // RISKMASTER_PF_IMPORT 
    // RISKMASTER_PD_IMPORT
    /// </summary>
    // 
    public class RiskMasterImporter
        {

        private RiskMasterCvsDbLoader _riskMasterCvsDbLoader;

        private RiskMasterDSVParser _riskMasterDSVParser;

        private string _processDirectory;

        static private System.Text.StringBuilder _errorStringBuilder = new System.Text.StringBuilder();

        public string ProcessDirectory
            {
            get { return _processDirectory; }
            set { _processDirectory = value; }
            }

        private string _archiveDirectory;

        public string ArchiveDirectory
            {
            get { return _archiveDirectory; }
            set { _archiveDirectory = value; }
            }

        private string _failDirectory;

        public string FailDirectory
            {
            get { return _failDirectory; }
            set { _failDirectory = value; }
            }

        private IList<String> _processedPfFiles;

        public IList<String> ProcessedPfFiles
            {
            get { return _processedPfFiles; }
            set { _processedPfFiles = value; }
            }

        private IList<String> _processedPdFiles;

        public IList<String> ProcessedPdFiles
            {
            get { return _processedPdFiles; }
            set { _processedPdFiles = value; }
            }

        private IList<String> _failedPfFiles;

        public IList<String> FailedPfFiles
            {
            get { return _failedPfFiles; }
            set { _failedPfFiles = value; }
            }

        private IList<String> _failedPdFiles;

        public IList<String> FailedPdFiles
            {
            get { return _failedPdFiles; }
            set { _failedPdFiles = value; }
            }

        static public System.Text.StringBuilder ErrorStringBuilder
            {
            get { return _errorStringBuilder; }
            set { _errorStringBuilder = value; }
            }
        public RiskMasterImporter(string processDirectory, string archiveDirectory, string failDirectory, char DSVDelimiter, char DSVQualifier)
            {
            this.ProcessDirectory = processDirectory;
            this.ArchiveDirectory = archiveDirectory;
            this.FailDirectory = failDirectory;
            this.ProcessedPfFiles = new List<string>();
            this.ProcessedPdFiles = new List<string>();
            this.FailedPfFiles = new List<string>();
            this.FailedPdFiles = new List<string>();
            this._riskMasterDSVParser = new RiskMasterDSVParser(DSVDelimiter, DSVQualifier);
            this._riskMasterCvsDbLoader = new RiskMasterCvsDbLoader();
            }

        public bool IterateParseAndInsertCvsFiles()
            {
            bool success = true;
            IEnumerable<string> pbDSVFiles = from file in Directory.EnumerateFiles(this.ProcessDirectory, "*.csv")
                                             where Path.GetFileName(file).ToLower().StartsWith("pb")
                                             select file;
            // Make certain the case of the letters in the file names match. make them all lower case
            IList<string> pbCvsFileList = renameFilesToLowerCase(pbDSVFiles);


            IEnumerable<string> pfDSVFiles = from file in Directory.EnumerateFiles(this.ProcessDirectory, "*.csv")
                                             where Path.GetFileName(file).ToLower().StartsWith("pf")
                                             select file;
            // Make certain the case of the letters in the file names match. make them all lower case
            IList<string> pfCvsFileList = renameFilesToLowerCase(pfDSVFiles);

            // iterate through the files, on each file call a DSV parser and then insert them into the database table RISKMASTER_PF_IMPORT 
            // gather any PF successes and display as succeeded
            var pdDSVFiles = from file in Directory.EnumerateFiles(this.ProcessDirectory, "*.csv")
                             where Path.GetFileName(file).ToLower().StartsWith("pd")
                             select file;
            IList<string> pdCvsFileList = renameFilesToLowerCase(pdDSVFiles);

            // iterate through the files, rename them to lower case. PB files are not parsed

            foreach (string pfCvsFile in pfCvsFileList)
                {

                // iterate through the files, on each file call a DSV parser and then insert them into the database table RISKMASTER_PF_IMPORT 

                string pfCvsFileName = Path.GetFileName(pfCvsFile);
                string DSVDirectoryPath = Path.GetDirectoryName(pfCvsFile);
                string pdCvsFileName = pfCvsFileName.Replace("pf", "pd");
                string pdDSVFile = DSVDirectoryPath + Path.DirectorySeparatorChar + pdCvsFileName;

                string pbCvsFileName = pfCvsFileName.Replace("pf", "pb");
                string pbDSVFile = DSVDirectoryPath + Path.DirectorySeparatorChar + pbCvsFileName;
                try
                    {
                    IList<XVar> pfDSVContentList;
                    if (pdCvsFileList.Contains(pdDSVFile) && File.Exists(pdDSVFile) && File.Exists(pfCvsFile))
                        {

                        pfDSVContentList = this._riskMasterDSVParser.getPfDSVContents(pfCvsFile);
                        IList<XVar> pdDSVContentList;
                        pdDSVContentList = this._riskMasterDSVParser.getPdDSVContents(pdDSVFile);

                        try
                            {
                            this._riskMasterCvsDbLoader.loadPfDSV(pfDSVContentList);
                            this._riskMasterCvsDbLoader.loadPdDSV(pdDSVContentList);

                            string pfArchiveFile = this.ArchiveDirectory + Path.DirectorySeparatorChar + pfCvsFileName;
                            File.Delete(pfArchiveFile);
                            File.Move(pfCvsFile, pfArchiveFile);
                            ProcessedPfFiles.Add(pfCvsFileName);

                            string pdArchiveFile = this.ArchiveDirectory + Path.DirectorySeparatorChar + pdCvsFileName;
                            File.Delete(pdArchiveFile);
                            File.Move(pdDSVFile, pdArchiveFile);
                            ProcessedPdFiles.Add(pdCvsFileName);

                            if (File.Exists(pbDSVFile))
                                {
                                string pbArchiveFile = this.ArchiveDirectory + Path.DirectorySeparatorChar + pbCvsFileName;

                                File.Delete(pbArchiveFile);
                                File.Move(pbDSVFile, pbArchiveFile);
                                }
                            else

                                {
                                ErrorStringBuilder.Append(pbDSVFile + " does not exist\n");
                                }
                            }
                        catch (Exception ex)
                            {
                            //remove all records that were added
                            this._riskMasterCvsDbLoader.removePdDSV(pdDSVContentList);
                            //remove all records that were added
                            this._riskMasterCvsDbLoader.removePfDSV(pfDSVContentList);
                            throw ex;
                            }
                        }
                    else
                        {
                        string pfFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pfCvsFileName;
                        if (File.Exists(pfCvsFile))
                            {
                            File.Delete(pfFailFile);
                            File.Move(pfCvsFile, pfFailFile);
                            }
                        else
                            {
                            ErrorStringBuilder.Append(pfCvsFile + " does not exist\n");
                            }
                        FailedPfFiles.Add(pfCvsFileName);


                        string pdFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pdCvsFileName;
                        if (File.Exists(pdDSVFile))
                            {
                            File.Delete(pdFailFile);
                            File.Move(pdDSVFile, pdFailFile);
                            }
                        else

                            {
                            ErrorStringBuilder.Append(pdDSVFile + " does not exist\n");
                            }

                        FailedPdFiles.Add(pdCvsFileName);

                        string pbFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pbCvsFileName;
                        if (File.Exists(pbDSVFile))
                            {
                            File.Delete(pbFailFile);
                            File.Move(pbDSVFile, pbFailFile);
                            }
                        else
                            {
                            ErrorStringBuilder.Append(pbDSVFile + " does not exist2\n");
                            }

                        success = false;
                        }
                    }
                catch (Exception ex)
                    {

                    string pfFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pfCvsFileName;
                    if (File.Exists(pfCvsFile))
                        {
                        File.Delete(pfFailFile);
                        File.Move(pfCvsFile, pfFailFile);
                        FailedPfFiles.Add(pfCvsFileName);
                        }


                    string pdFailFile = this.FailDirectory + Path.DirectorySeparatorChar + pdCvsFileName;
                    if (File.Exists(pdDSVFile))
                        {
                        File.Delete(pdFailFile);
                        File.Move(pdDSVFile, pdFailFile);
                        FailedPdFiles.Add(pdCvsFileName);
                        }


                    ErrorStringBuilder.AppendFormat("An exception ({0}) occurred.\n", ex.GetType().Name);
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

                    success = false;
                    }
                }
            return success;
            }

        public IList<string> renameFilesToLowerCase(IEnumerable<string> DSVFiles)
            {
            IList<string> renamedFilesList = new List<string>();
            foreach (string DSVFile in DSVFiles)
                {
                string DSVFileName = Path.GetFileName(DSVFile);
                string DSVDirectoryPath = Path.GetDirectoryName(DSVFile);
                if (!DSVFileName.Equals(DSVFileName.ToLower()))
                    {
                    string newFilePath = DSVDirectoryPath + Path.DirectorySeparatorChar + DSVFileName.ToLower();
                    File.Move(DSVFile, newFilePath);
                    renamedFilesList.Add(newFilePath);
                    }
                else
                    {
                    renamedFilesList.Add(DSVFile);
                    }
                }
            return renamedFilesList;
            }
        public void deletePbCvsFiles(IEnumerable<string> DSVFiles)
            {

            foreach (string DSVFile in DSVFiles)
                {

                if (File.Exists(DSVFile))
                    {
                    File.Delete(DSVFile);
                    }
                }
            }
        }
    }
