using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {
    public class ProcessResult
        {
        private string _standardOutput = null;
        private string _standardError = null;
        private int _exitCode = 1;

        public string StandardOutput
            {
            get { return _standardOutput; }
            set { _standardOutput = value; }
            }
        public string StandardError 
            {
            get { return _standardError; }
            set { _standardError = value; }
            }
        public int ExitCode 
            { 
            get { return _exitCode; }
            set { _exitCode = value; } 
            }


        }
    }
