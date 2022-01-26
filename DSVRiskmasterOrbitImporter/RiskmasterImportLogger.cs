using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {
    public class RiskmasterImportLogger
        {
        private string LogDirpath { get; set; }
        private string BaseLogFileName { get; set; }

        private String logFilepath;
        public string LogFilepath
            {
            get { return logFilepath; }
            set { logFilepath = value; }
            }

        private static object lockLog = new Object();

        public RiskmasterImportLogger(string dirPath, string logFileName)
            {
            LogDirpath = dirPath;
            BaseLogFileName = logFileName;
            char separator = System.IO.Path.DirectorySeparatorChar;
            String currentDateString = DateTime.Now.ToString("yyyyMMdd");
            logFilepath = String.Format("{0}{1}{2}-{3}.log", LogDirpath, System.IO.Path.DirectorySeparatorChar, BaseLogFileName, currentDateString);
            }


        public void log(StringBuilder text)
            {
            lock (lockLog)
                {

                using (System.IO.FileStream fileStream = System.IO.File.Open(logFilepath, System.IO.FileMode.Append, System.IO.FileAccess.Write, System.IO.FileShare.None))
                    {

                    using (System.IO.StreamWriter logStream = new System.IO.StreamWriter(fileStream))
                        {
                        logStream.WriteLine(text);
                        }
                    }
                }
            }
        }
    }
