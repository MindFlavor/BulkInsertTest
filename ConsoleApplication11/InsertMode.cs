using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication11
{
    public enum InsertMode
    {
        Invalid,
        BulkInsert_NoTablock,
        BulkInsert_Tablock,
        BulkInsert_SingleEntry_NoTablock,
        BulkInsert_SingleEntry_Tablock,
        StoredProcedure,
        TSQL,
        TSQL_Prepared
    }
}
