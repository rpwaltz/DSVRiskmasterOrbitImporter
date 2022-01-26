using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace runnerDotNet
    {
    public class XSession
        {
        static XSession session = new XSession();

        public static XSession Session
            {
            get { return session; }
            }

        public XVar this[object key]
            {
            get { return XVar.Pack(key); }
            }
        }
    }
