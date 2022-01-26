using System;
using System.Collections.Generic;
using System.IO;
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
    public class RiskMasterCvsDbLoader
        {
        OracleDBFacade oracleDBFacade = new OracleDBFacade();
        public RiskMasterCvsDbLoader()
            {
            }
        public void loadPdDSV(IList<XVar> pdDSVList)
            {

            foreach (XVar pdCvsDataRow in pdDSVList)
                {
                oracleDBFacade.Insert("xxcok.xxcok_rm_import_pd", pdCvsDataRow);
                }

            }
        public void loadPfDSV(IList<XVar> pfDSVList)
            {

            foreach (XVar pfCvsDataRow in pfDSVList)
                {
                oracleDBFacade.Insert("xxcok.xxcok_rm_import_pf", pfCvsDataRow);
                }

            }
        public void removePfDSV(IList<XVar> pfDSVList)
            {

            foreach (XVar pfCvsDataRow in pfDSVList)
                {

                    oracleDBFacade.Delete("xxcok.xxcok_rm_import_pf", pfCvsDataRow);
                }

            }
        public void removePdDSV(IList<XVar> pdDSVList)
            {

            foreach (XVar pdCvsDataRow in pdDSVList)
                {

                    oracleDBFacade.Delete("xxcok.xxcok_rm_import_pd", pdCvsDataRow);

                }

            }

        }
    }