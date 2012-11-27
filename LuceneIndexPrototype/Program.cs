using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace LuceneIndexPrototype
{
    class Program
    {
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();

            var visitIndexDef = new IndexDefinition()
            {
                Name = "Visits",
                Indexes = { 
                            { "RowVersion", FieldIndexing.No } 
                           ,{ "VisitDateTime", FieldIndexing.Default}
                           ,{ "ExternalId", FieldIndexing.NotAnalyzed } 
                           ,{ "ProviderOrganizationGuid", FieldIndexing.NotAnalyzed } 
                           ,{ "PatientGuid", FieldIndexing.NotAnalyzed } 
                           ,{ "ProviderLocationGuid", FieldIndexing.NotAnalyzed } 
                           ,{ "AssignedProviderGuid", FieldIndexing.NotAnalyzed } 
                           ,{ "AssignedProviderServiceGuid", FieldIndexing.NotAnalyzed } 
                           ,{ "AssignedAppointmentLocationGuid", FieldIndexing.NotAnalyzed } 
                           ,{ "Seen", FieldIndexing.NotAnalyzed } 
                           ,{ "CheckedOut", FieldIndexing.NotAnalyzed } 
                           ,{ "Viewed", FieldIndexing.NotAnalyzed } 
                           ,{ "Started", FieldIndexing.NotAnalyzed } 
                          }
                           ,
                Stores = { { "RowVersion", FieldStorage.Yes } },
                SortOptions = { { "VisitDateTime", SortOptions.Long } },
            };

            var storage = new IndexStorage();
            var visitIndex = storage.OpenIndexOnStartup(visitIndexDef);

            var stopwatch = new Stopwatch();
            long totalCount = 0;
            long totalEllapsed = 0;
            using (var conn = new SqlConnection("server=cw-sit2-platdb.cw.local;database=providerportal;user id=cwdev;password=M5DrECr!c$"))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    var row = new object[17];
                    var doc = new VisitIndexableDocument(row);
                    var uid_encounter = Guid.Empty;
                    cmd.CommandText = visitQuery;
                    cmd.Parameters.AddWithValue("@uid_encounter", uid_encounter);
                    while (true)
                    {
                        cmd.Parameters["@uid_encounter"].Value = uid_encounter;
                        using (var reader = cmd.ExecuteReader())
                        {
                            int batchCount = 0;
                            if (!reader.HasRows)
                            {
                                break;
                            }
                            var documents = AsEnumerable(reader.Read, () =>
                            {
                                totalCount++;
                                batchCount++;
                                uid_encounter = reader.GetGuid(0);
                                reader.GetValues(row);
                                return doc;
                            });
                            stopwatch.Restart();
                            visitIndex.IndexDocuments(documents);
                            stopwatch.Stop();
                            totalEllapsed += stopwatch.ElapsedMilliseconds;
                            Console.WriteLine(string.Format("Last Batch {0}/{1} = {2} : Overall {3}/{4} = {5}", batchCount, stopwatch.ElapsedMilliseconds, (double)batchCount / (double)stopwatch.ElapsedMilliseconds, totalCount, totalEllapsed, (double)totalCount / (double)totalEllapsed));
                        }
                    }
                }
            }

            //var docs = new List<IndexableDocument>();
            //for (int i = 0; i < 100; i++)
            //{
            //    docs.AddRange(new[] { 
            //    new IndexableDocument() { DocumentId = i.ToString(), Fields = { { "LastName", "Daigle" }, 
            //                                                                {"FirstName" ,"Joseph" } } },
            //    });
            //}
            //for (int i = 0; i < 100; i++)
            //{
            //    visitIndex.IndexDocuments(docs);
            //}
            //visitIndex.Flush();




            //visitIndex.MergeSegments();
            //var reader = visitIndex.indexWriter.GetReader();
            //var searcher = new IndexSearcher(reader);
            //Query query = new TermQuery(new Term("uid_ProviderOrganization", "377fe1b9-aeb0-45a9-a4aa-7dac08377d6b"));
            //query = NumericRangeQuery.NewLongRange("VisitDateTime_Range", new DateTime(2009, 11, 18).Ticks, new DateTime(2009, 12, 18).Ticks, true, true);
            //var stopwatch = new Stopwatch();
            //stopwatch.Start();
            //var results = searcher.Search(query, null, 20, new Sort(new SortField("VisitDateTime_Range", SortField.LONG)));
            //stopwatch.Stop();
            //stopwatch.Restart();
            //results = searcher.Search(query, null, 20, new Sort(new SortField("VisitDateTime_Range", SortField.LONG)));
            //stopwatch.Stop();
        }

        public static IEnumerable<IndexableDocument> AsEnumerable(Func<bool> next, Func<IndexableDocument> getNext)
        {
            while (next())
            {
                yield return getNext();
            }
        }

        public const string visitQuery = @"
SELECT
    TOP 1000
    uid_encounter,  -- 0
    EncounterID, 
    ExternalId, 
    uid_ProviderOrganization, --3
    uid_Patient, 
    uid_ProviderLocation, 
    dt_Visit, -- 6
    COALESCE(OriginalAppointmentDateTime, dt_visit) as AppointmentDateTime,
    bit_Appointment,
    cast([rowversion] as bigint),  -- 9
    cast(CASE WHEN dt_Seen IS NOT NULL THEN 1 ELSE 0 END as bit) AS seen,
    cast(CASE WHEN dt_CheckOut IS NOT NULL THEN 1 ELSE 0 END as bit) AS checkout,
    cast(CASE WHEN ViewedDateTime IS NOT NULL THEN 1 ELSE 0 END as bit) AS viewed,
    cast(CASE WHEN StartedDateTime IS NOT NULL THEN 1 ELSE 0 END as bit) AS started,
    AssignedProviderId, -- 14
    AssignedProviderServiceId,
    AssignedAppointmentLocationId
FROM tEncounter (NOLOCK)
WHERE tEncounter.uid_Encounter > @uid_encounter
AND tEncounter.bit_Appointment = 0
ORDER BY tEncounter.uid_Encounter
";
    }
}
