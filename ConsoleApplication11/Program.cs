using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication11
{
    class Params
    {
        public int ThreadNumber;
        public CommandLineParams GenericParams;
        public ManualResetEvent InsertBlock;
        public ManualResetEvent AllocationCompleted;
        public ManualResetEvent InsertCompleted;
        public InsertMode InsertMode;
    }

    class Program
    {

        static void Main(string[] args)
        {
            #region Command line 
            CommandLineParams cmp = new CommandLineParams();

            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-t":
                        cmp.ThreadNumber = long.Parse(args[i + 1]);
                        break;
                    case "-r":
                        cmp.RowsPerThread = long.Parse(args[i + 1]);
                        break;
                    case "-c":
                        cmp.ConnectionString = args[i + 1];
                        break;
                    case "-b":
                        cmp.BatchSize = long.Parse(args[i + 1]);
                        break;
                    case "-p":
                        cmp.UsePartitionedTable = bool.Parse(args[i + 1]);
                        break;
                    case "-i":
                        InsertMode im;
                        if (Enum.TryParse<InsertMode>(args[i + 1], out im))
                            cmp.InsertMode = im;
                        break;
                }
            }

            if (cmp.ThreadNumber == -1 || cmp.RowsPerThread == -1 || cmp.ConnectionString == string.Empty || cmp.InsertMode == InsertMode.Invalid)
            {
                Console.WriteLine("Syntax error. Syntax:\nInsertTester -t <thread num> -r <rows per thread> -c <connection string> -i <insert mode> |-b <batch size>| |-p user partitioned table|\n");
                Console.WriteLine("\t-b\tOptional batch size (default {0:N0}). Used in bulk inserts only.", CommandLineParams.DEFAULT_BATCH_SIZE);
                Console.WriteLine("\t-p\tOptional use partitioned table (true or false, default {0:S0}).", CommandLineParams.DEFAULT_USE_PARTITIONED_TABLE.ToString());
                Console.WriteLine("\nInsert modes available:");
                foreach (var t in Enum.GetNames(typeof(InsertMode)).Where(t => t != "Invalid"))
                {
                    Console.WriteLine("\t" + t);
                }

                return;
            }
            #endregion


            ManualResetEvent StartInsert = new ManualResetEvent(false);
            List<Params> lParams = new List<Params>();

            for (int i = 0; i < cmp.ThreadNumber; i++)
            {
                ParameterizedThreadStart pts = new ParameterizedThreadStart(ThCode);
                Thread t = new Thread(pts);

                Params p = new Params()
                {
                    ThreadNumber = i,
                    GenericParams = cmp,
                    InsertBlock = StartInsert,
                    AllocationCompleted = new ManualResetEvent(false),
                    InsertCompleted = new ManualResetEvent(false),
                    InsertMode = cmp.InsertMode
                };

                t.Start(p);

                lParams.Add(p);
            }

            // wait for allocations to finish
            lParams.ForEach(p => p.AllocationCompleted.WaitOne());
            Console.WriteLine("Thread allocation phase completed. Signaling insert");

            // start load
            DateTime dtStart = DateTime.Now;
            StartInsert.Set();

            // wait for inserts to finish       
            lParams.ForEach(p => p.InsertCompleted.WaitOne());
            DateTime dtEnd = DateTime.Now;

            var elapsed = dtEnd - dtStart;
            Console.WriteLine("Inserted {0:N0} rows with {1:N0} threads. Time taken {2:S} ({3:N0} rows/second)",
                            cmp.RowsPerThread * cmp.ThreadNumber,
                            cmp.ThreadNumber,
                            elapsed.ToString(),
                            ((double)cmp.RowsPerThread * cmp.ThreadNumber) / elapsed.TotalSeconds);

            Console.WriteLine("All thread finished inserting rows.");
        }

        static void ThCode(object o)
        {
            Params p = (Params)o;

            DataTable tbl = new DataTable();
            tbl.Columns.Add("ID");
            tbl.Columns.Add("Testo");
            tbl.Columns.Add("Dt");

            Console.WriteLine(string.Format("Thread {0:N0} buffer allocation starting (allocating {1:N0} rows)", p.ThreadNumber, p.GenericParams.RowsPerThread));
            for (int i = 0; i < p.GenericParams.RowsPerThread; i++)
            {
                tbl.Rows.Add(new object[]
                {
                    i + (p.ThreadNumber * p.GenericParams.RowsPerThread),
                    "Prova " + i,
                    DateTime.Now
                });
            }

            p.AllocationCompleted.Set();
            Console.WriteLine(string.Format("Thread {0:N0} buffer allocation completed, waiting for gateway", p.ThreadNumber));

            p.InsertBlock.WaitOne();

            try
            {
                switch (p.InsertMode)
                {
                    case InsertMode.BulkInsert_SingleEntry_NoTablock:
                        #region BulkInsert_SingleEntry_NoTablock
                        foreach (DataRow row in tbl.Rows)
                        {
                            using (SqlBulkCopy cp = new SqlBulkCopy(
                                p.GenericParams.ConnectionString,
                                SqlBulkCopyOptions.KeepNulls))
                            {
                                cp.BulkCopyTimeout = 60 * 60 * 1000; // 1 hour
                                cp.DestinationTableName = p.GenericParams.UsePartitionedTable ? "tblHeapPartition" : "tblHeap";
                                cp.WriteToServer(new System.Data.DataRow[] { row });
                            }
                        }
                        break;
                    #endregion

                    case InsertMode.BulkInsert_SingleEntry_Tablock:
                        #region BulkInsert_SingleEntry_NoTablock
                        foreach (DataRow row in tbl.Rows)
                        {
                            using (SqlBulkCopy cp = new SqlBulkCopy(
                                p.GenericParams.ConnectionString,
                                SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock))
                            {
                                cp.BulkCopyTimeout = 60 * 60 * 1000; // 1 hour
                                cp.DestinationTableName = p.GenericParams.UsePartitionedTable ? "tblHeapPartition" : "tblHeap";
                                cp.WriteToServer(new System.Data.DataRow[] { row });
                            }
                        }
                        break;
                    #endregion

                    case InsertMode.BulkInsert_NoTablock:
                        #region BulkInsert_NoTablock
                        using (SqlBulkCopy cp = new SqlBulkCopy(
                            p.GenericParams.ConnectionString,
                            SqlBulkCopyOptions.KeepNulls))
                        {
                            cp.BulkCopyTimeout = 60 * 60 * 1000; // 1 hour
                            cp.DestinationTableName = p.GenericParams.UsePartitionedTable ? "tblHeapPartition" : "tblHeap";
                            cp.WriteToServer(tbl);
                        }
                        break;
                    #endregion

                    case InsertMode.BulkInsert_Tablock:
                        #region BulkInsert_Tablock
                        using (SqlBulkCopy cp = new SqlBulkCopy(
                            p.GenericParams.ConnectionString,
                            SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock))
                        {
                            cp.BulkCopyTimeout = 60 * 60 * 1000; // 1 hour
                            cp.DestinationTableName = p.GenericParams.UsePartitionedTable ? "tblHeapPartition" : "tblHeap";
                            cp.WriteToServer(tbl);
                        }
                        break;
                    #endregion

                    case InsertMode.StoredProcedure:
                        #region StoredProcedure
                        foreach (DataRow row in tbl.Rows)
                        {
                            using (SqlConnection conn = new SqlConnection(p.GenericParams.ConnectionString))
                            {
                                conn.Open();

                                string spName = p.GenericParams.UsePartitionedTable ? "spspInsertPartition" : "spspInsert";

                                using (SqlCommand cmd = new SqlCommand(spName, conn))
                                {
                                    cmd.CommandType = CommandType.StoredProcedure;

                                    SqlParameter param = new SqlParameter("@ID", SqlDbType.Int, 4);
                                    param.Value = row[0];
                                    cmd.Parameters.Add(param);

                                    param = new SqlParameter("@Testo", SqlDbType.NVarChar, 255);
                                    param.Value = row[1];
                                    cmd.Parameters.Add(param);

                                    param = new SqlParameter("@Dt", SqlDbType.DateTime, 8);
                                    param.Value = row[2];
                                    cmd.Parameters.Add(param);

                                    cmd.Prepare();

                                    cmd.ExecuteNonQuery();
                                }
                            }
                        }
                        break;
                    #endregion

                    case InsertMode.TSQL_Prepared:
                        #region TSQL_Prepared
                        {
                            string tableName = p.GenericParams.UsePartitionedTable ? "tblHeapPartition" : "tblHeap";
                            string tsql = string.Format(@"INSERT INTO {0:S}(ID, Testo, Dt)
                                        VALUES(@ID, @Testo, @Dt);", tableName);

                            foreach (DataRow row in tbl.Rows)
                            {
                                using (SqlConnection conn = new SqlConnection(p.GenericParams.ConnectionString))
                                {
                                    conn.Open();

                                    using (SqlCommand cmd = new SqlCommand(tsql, conn))
                                    {
                                        cmd.CommandType = CommandType.Text;

                                        SqlParameter param = new SqlParameter("@ID", SqlDbType.Int, 4);
                                        param.Value = row[0];
                                        cmd.Parameters.Add(param);

                                        param = new SqlParameter("@Testo", SqlDbType.NVarChar, 255);
                                        param.Value = row[1];
                                        cmd.Parameters.Add(param);

                                        param = new SqlParameter("@Dt", SqlDbType.DateTime, 8);
                                        param.Value = row[2];
                                        cmd.Parameters.Add(param);

                                        cmd.Prepare();

                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                        break;
                    #endregion

                    case InsertMode.TSQL:
                        #region TSQL
                        {
                            string tableName = p.GenericParams.UsePartitionedTable ? "tblHeapPartition" : "tblHeap";

                            foreach (DataRow row in tbl.Rows)
                            {
                                using (SqlConnection conn = new SqlConnection(p.GenericParams.ConnectionString))
                                {
                                    conn.Open();

                                    string tsql = string.Format(@"INSERT INTO {0:S}(ID, Testo, Dt)
                                        VALUES({1:D}, '{2:S}', '{3:S}');",
                                            tableName,
                                            row[0], row[1], row[2].ToString());

                                    using (SqlCommand cmd = new SqlCommand(tsql, conn))
                                    {
                                        cmd.CommandType = CommandType.Text;
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                            break;
                        }
                    #endregion

                    default:
                        throw new Exception(string.Format("Unsupported load method: {0:S}.", p.InsertMode.ToString()));
                }
            }
            catch (Exception exce)
            {
                Console.WriteLine("Unhandled exception in thread {0:N0}: {1:S}", p.ThreadNumber, exce.ToString());
                p.InsertCompleted.Set();
                return;
            }

            p.InsertCompleted.Set();
            Console.WriteLine(string.Format("Thread {0:N0} insert completed", p.ThreadNumber));
        }
    }
}
