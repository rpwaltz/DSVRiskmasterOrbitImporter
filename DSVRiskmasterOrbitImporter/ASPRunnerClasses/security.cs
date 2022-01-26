using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {
    public partial class Security : XClass
        {
        public static XVar getUserName()
            {
            return XSession.Session["rwaltz@knoxvilletn.gov"];
            }
        }
    }
