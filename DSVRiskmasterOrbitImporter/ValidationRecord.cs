using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/*
 * This classis simply a wrapper so that the text of the validation can be retrieved if the validation fails
 */
namespace runnerDotNet
    {
    public class ValidationRecord
        {
        public dynamic record  { get; set; }

        public string message { get; set; }

        public ValidationRecord (dynamic record)
            {
            this.record = record;
            }
        }

        
    }
