using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

namespace runnerDotNet
    {
    public class RiskMasterImportDeletion
        {
        OracleDBFacade oracleDBFacade = new OracleDBFacade();
        public void delete_imported_record(string pf_id)
            {
            CultureInfo provider = new CultureInfo("en-US");
            NumberStyles styles = NumberStyles.AllowLeadingWhite | NumberStyles.AllowThousands;
            Int32 pfIdInt = 0;
            if (Int32.TryParse(pf_id, styles, provider, out pfIdInt))
                {

                String deletePDString = String.Format("DELETE FROM XXCOK.XXCOK_RM_IMPORT_PD WHERE EXISTS(select XXCOK.XXCOK_RM_IMPORT_PF.pf_id " +
                                " from XXCOK.XXCOK_RM_IMPORT_PF where XXCOK.XXCOK_RM_IMPORT_PF.pf_id = {0} " +
                                " AND XXCOK.XXCOK_RM_IMPORT_PD.IMPORT_ID = XXCOK.XXCOK_RM_IMPORT_PF.IMPORT_ID " +
                                " AND XXCOK.XXCOK_RM_IMPORT_PD.IMPORT_DATE = XXCOK.XXCOK_RM_IMPORT_PF.IMPORT_DATE " +
                                " AND XXCOK.XXCOK_RM_IMPORT_PD.PAYMENT_ID = XXCOK.XXCOK_RM_IMPORT_PF.PAYMENT_ID)", pfIdInt);

                String deletePFString = String.Format("DELETE FROM XXCOK.XXCOK_RM_IMPORT_PF WHERE PF_ID = {0}", pfIdInt);

                oracleDBFacade.Exec(deletePDString);
                oracleDBFacade.Exec(deletePFString);
                }
            else
                {
                throw new Exception(pf_id + " is not a valid number ");
                }
            }

        }
    }
