using System;
using System.Collections.Generic;

namespace LuceneIndexPrototype
{
    public abstract class IndexableDocument
    {
        public abstract string DocumentId { get; }
        public abstract IEnumerable<KeyValuePair<string, object>> Fields();
    }

    public class VisitIndexableDocument : IndexableDocument
    {
        private object[] row;

        public VisitIndexableDocument(object[] row)
        {
            this.row = row;
        }

        public override string DocumentId
        {
            get { return row[0].ToString(); }
        }

        public override IEnumerable<KeyValuePair<string, object>> Fields()
        {
            yield return new KeyValuePair<string, object>("RowVersion", row[9].ToString());
            yield return new KeyValuePair<string, object>("VisitDateTime", ((DateTime)row[6]).Ticks);
            yield return new KeyValuePair<string, object>("ExternalId", row[3] != null ? row[3].ToString() : null);
            yield return new KeyValuePair<string, object>("ProviderOrganizationGuid", row[3].ToString().ToLower());
            yield return new KeyValuePair<string, object>("PatientGuid", row[4].ToString().ToLower());
            yield return new KeyValuePair<string, object>("ProviderLocationGuid", row[5].ToString().ToLower());
            yield return new KeyValuePair<string, object>("AssignedProviderGuid", row[14] != null ? row[14].ToString() : null);
            yield return new KeyValuePair<string, object>("AssignedProviderServiceGuid", row[15] != null ? row[15].ToString() : null);
            yield return new KeyValuePair<string, object>("AssignedAppointmentLocationGuid", row[16] != null ? row[16].ToString() : null);
            yield return new KeyValuePair<string, object>("Seen", row[10]);
            yield return new KeyValuePair<string, object>("CheckedOut", row[11]);
            yield return new KeyValuePair<string, object>("Viewed", row[12]);
            yield return new KeyValuePair<string, object>("Started", row[13]);
        }
    }
}