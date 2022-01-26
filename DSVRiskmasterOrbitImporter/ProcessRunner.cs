using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
namespace runnerDotNet
    {
    public class ProcessRunner
        {
        public static ProcessResult RunWinProcess(string executableFilepath, string executableArguments)
            {
            ProcessStartInfo startInfo = new ProcessStartInfo();
            ProcessResult processResult = new ProcessResult();
            startInfo.UseShellExecute = false;
            startInfo.CreateNoWindow = true;
            startInfo.RedirectStandardOutput = true;
            startInfo.RedirectStandardError = true;
            startInfo.FileName = executableFilepath; // @"C:\Users\rwaltz\Documents\Git_Projects\KCGRiskmasterFTP\KCGRiskmasterFTP\bin\Debug\KCGRiskmasterFTP.exe";
            startInfo.Arguments = executableArguments; // @"-w -c F:\WCEDI_Exports\config\SessionOptions.config";
            Process winProcess = Process.Start(startInfo);
            processResult.StandardOutput = winProcess.StandardOutput.ReadToEnd();
            processResult.StandardError = winProcess.StandardError.ReadToEnd();
            winProcess.WaitForExit();
            processResult.ExitCode = winProcess.ExitCode;
            return processResult;
            }
        }
    }
