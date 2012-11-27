using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                            { "VisitDateTime", FieldIndexing.d
 }
                           ,{ "uid_ProviderOrganization", FieldIndexing.NotAnalyzed } 
                           ,{ "uid_Patient", FieldIndexing.NotAnalyzed } 
                           ,{ "uid_ProviderLocation", FieldIndexing.NotAnalyzed } 
                          }
                           ,
                SortOptions = { { "VisitDateTime", SortOptions.Long } }
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
                    var uid_encounter = Guid.Empty;
                    cmd.CommandText = visitQuery;
                    cmd.Parameters.AddWithValue("@uid_encounter", uid_encounter);
                    while (true)
                    {
                        cmd.Parameters["@uid_encounter"].Value = uid_encounter;
                        var docs = new List<IndexableDocument>();
                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.HasRows)
                            {
                                break;
                            }
                            while (reader.Read())
                            {
                                totalCount++;
                                uid_encounter = reader.GetGuid(0);
                                var row = new object[17];
                                reader.GetValues(row);
                                docs.Add(new VisitIndexableDocument(row));
                            }
                            stopwatch.Restart();
                            visitIndex.IndexDocuments(docs);
                            stopwatch.Stop();
                            totalEllapsed += stopwatch.ElapsedMilliseconds;
                            Console.WriteLine(string.Format("Last Batch {0}/{1} = {2} : Overall {3}/{4} = {5}", docs.Count, stopwatch.ElapsedMilliseconds, (double)docs.Count / (double)stopwatch.ElapsedMilliseconds, totalCount, totalEllapsed, (double)totalCount / (double)totalEllapsed));
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
    [rowversion],
    CASE WHEN dt_Seen IS NOT NULL THEN 1 ELSE 0 END AS seen,
    CASE WHEN dt_CheckOut IS NOT NULL THEN 1 ELSE 0 END AS checkout,
    CASE WHEN ViewedDateTime IS NOT NULL THEN 1 ELSE 0 END AS viewed,
    CASE WHEN StartedDateTime IS NOT NULL THEN 1 ELSE 0 END AS started,
    AssignedProviderId,
    AssignedProviderServiceId,
    AssignedAppointmentLocationId
FROM tEncounter (NOLOCK)
WHERE tEncounter.uid_Encounter > @uid_encounter
ORDER BY tEncounter.uid_Encounter
";
    }
}
