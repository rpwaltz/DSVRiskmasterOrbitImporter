using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {

    // Represents the File that will be sent to WCEDI
    // It is a Delimiter Separated Value file that contains the header
    // The delimiter is a |
    // There is no qualifier for strings defined

    //
    public class WcediDSVFile
        {
        
        private static object lockFile = new Object();

        private StringBuilder dsvStringBuilder = new StringBuilder();

        public StringBuilder DSVStringBuilder
            {
            get { return dsvStringBuilder; }
            set { dsvStringBuilder = value; }
            }

        private string wcediFilepath;
        public string WcediFilepath
            {
            get { return wcediFilepath; }
            set { wcediFilepath = value; }
            }

        private int count = 0;



        public WcediDSVFile(string wcediFilepath, string headerRow)
            {
            this.wcediFilepath = wcediFilepath;
            DSVStringBuilder.AppendLine(headerRow);
            }

        public void Add(string DSVLine)
            {
            DSVStringBuilder.AppendLine(DSVLine);
            ++count;
            }

        public int Count()
            {
            return count;
            }

        public void WriteToFile()
            {
            lock (lockFile)
                {

                using (System.IO.FileStream fileStream = System.IO.File.Open(wcediFilepath, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.Write, System.IO.FileShare.Delete | System.IO.FileShare.Read))
                    {
                    using (System.IO.StreamWriter streamWriter = new System.IO.StreamWriter(fileStream))
                        {
                        streamWriter.Write(DSVStringBuilder.ToString());
                        }
                    }
                }
            }

        }
    }
