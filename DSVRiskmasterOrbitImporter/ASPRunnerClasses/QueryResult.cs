using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using runnerDotNet;
namespace runnerDotNet
    {
    public partial class QueryResult : DataResult
        {
        protected dynamic connectionObj;
        protected dynamic handle;
        protected dynamic state = XVar.Pack(-1);
        protected static bool skipQueryResultCtor = false;
        public QueryResult(dynamic _param_connectionObj, dynamic _param_qHandle)
            {
            if (skipQueryResultCtor)
                {
                skipQueryResultCtor = false;
                return;
                }
            #region pass-by-value parameters
            dynamic connectionObj = XVar.Clone(_param_connectionObj);
            dynamic qHandle = XVar.Clone(_param_qHandle);
            #endregion

            this.connectionObj = XVar.Clone(connectionObj);
            this.handle = XVar.Clone(qHandle);
            }
        public override XVar getQueryHandle()
            {
            return this.handle;
            }
        public override XVar fetchAssoc()
            {
            dynamic ret = null;
            if (this.state == 1)
                {
                return null;
                }
            if (this.state == 0)
                {
                this.state = XVar.Clone(-1);
                return numericToAssoc((XVar)(this.data));
                }
            ret = XVar.Clone(this.fetch_array((XVar)(this.handle)));
            if (XVar.Pack(this.fieldSubs))
                {
                ret = XVar.Clone(substituteFields((XVar)(ret)));
                }
            this.state = XVar.Clone((XVar.Pack(ret) ? XVar.Pack(-1) : XVar.Pack(1)));
            return ret;
            }
        public override XVar fetchNumeric()
            {
            dynamic ret = null;
            if (this.state == 1)
                {
                return null;
                }
            if (this.state == 0)
                {
                this.state = XVar.Clone(-1);
                return this.data;
                }
            ret = XVar.Clone(this.fetch_numarray((XVar)(this.handle)));
            this.state = XVar.Clone((XVar.Pack(ret) ? XVar.Pack(-1) : XVar.Pack(1)));
            return ret;
            }
        public override XVar closeQuery()
            {
            this.connectionObj.closeQuery((XVar)(this.handle));
            return null;
            }
        public override XVar numFields()
            {
            return this.connectionObj.num_fields((XVar)(this.handle));
            }
        public override XVar fieldName(dynamic _param_offset)
            {
            #region pass-by-value parameters
            dynamic offset = XVar.Clone(_param_offset);
            #endregion

            return this.connectionObj.field_name((XVar)(this.handle), (XVar)(offset));
            }
        public override XVar seekRecord(dynamic _param_n)
            {
            #region pass-by-value parameters
            dynamic n = XVar.Clone(_param_n);
            #endregion

            this.connectionObj.seekRecord((XVar)(this.handle), (XVar)(n));
            return null;
            }
        public override XVar eof()
            {
            prepareRecord();
            return this.state == 1;
            }
        protected virtual XVar internalFetch()
            {
            if (this.state == 1)
                {
                return null;
                }
            fillColumnNames();
            this.data = XVar.Clone(this.fetch_numarray((XVar)(this.handle)));
            this.state = XVar.Clone((XVar.Pack(this.data) ? XVar.Pack(0) : XVar.Pack(1)));
            return null;
            }
        protected override XVar fillColumnNames()
            {
            dynamic fname = null, i = null, nFields = null;
            if (XVar.Pack(this.fieldNames))
                {
                return null;
                }
            nFields = XVar.Clone(numFields());
            i = new XVar(0);
            for (; i < nFields; ++(i))
                {
                fname = XVar.Clone(fieldName((XVar)(i)));
                this.fieldNames.InitAndSetArrayItem(fname, null);
                this.fieldMap.InitAndSetArrayItem(i, fname);
                this.upperMap.InitAndSetArrayItem(i, strtoupper((XVar)(fname)));
                }
            return null;
            }
        public override XVar func_next()
            {
            prepareRecord();
            internalFetch();
            return null;
            }
        protected override XVar prepareRecord()
            {
            if (this.state == -1)
                {
                internalFetch();
                }
            return this.state != 1;
            }
        public override XVar getData()
            {
            if (XVar.Pack(!(XVar)(prepareRecord())))
                {
                return null;
                }
            return numericToAssoc((XVar)(this.data));
            }
        public override XVar getNumData()
            {
            if (XVar.Pack(!(XVar)(prepareRecord())))
                {
                return null;
                }
            return this.data;
            }
        public virtual XVar count()
            {
            dynamic cnt = null, data = null;
            cnt = new XVar(0);
            while (XVar.Pack(data = XVar.Clone(fetchAssoc())))
                {
                ++(cnt);
                }
            return cnt;
            }

        private XVar fetch_array(dynamic qHanle)
            {
            //db_fetch_array
            if (qHanle != null)
                {
                RunnerDBReader reader = (RunnerDBReader)qHanle;
                if (reader.Read())
                    {
                    XVar result = new XVar();
                    for (int i = 0; i < reader.FieldCount; i++)
                        {
                        result.SetArrayItem(reader.GetName(i), reader[i]);
                        }
                    return result;
                    }
                }

            return XVar.Array();
            }

        public XVar fetch_numarray(dynamic qHanle)
            {
            //db_fetch_numarray
            if (qHanle != null)
                {
                RunnerDBReader reader = (RunnerDBReader)qHanle;
                if (reader.Read())
                    {
                    XVar result = new XVar();
                    for (int i = 0; i < reader.FieldCount; i++)
                        {
                        result.SetArrayItem(i, reader[i]);
                        }
                    return result;
                    }
                }

            return XVar.Array();
            }
        }
    }
