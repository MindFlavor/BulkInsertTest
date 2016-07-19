using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication11
{
    public class CommandLineParams
    {
        public const int DEFAULT_BATCH_SIZE = 100000;
        public const bool DEFAULT_USE_PARTITIONED_TABLE = false;

        public long ThreadNumber = -1;
        public long RowsPerThread = -1;
        public InsertMode InsertMode = InsertMode.Invalid;
        public string ConnectionString = string.Empty;
        public long BatchSize = DEFAULT_BATCH_SIZE;
        public bool UsePartitionedTable = DEFAULT_USE_PARTITIONED_TABLE;
    }
}
