using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Lucene.Net.Documents;

namespace LuceneIndexPrototype
{
    public class IndexableDocument
    {
        public IndexableDocument()
        {
            Fields = new Dictionary<string, object>();
        }
        public string DocumentId { get; set; }
        public Dictionary<string, object> Fields { get; set; }
    }

    public class VisitIndexableDocument : IndexableDocument
    {
        public VisitIndexableDocument()
        {

        }

        public VisitIndexableDocument(object[] row)
        {
            this.DocumentId = row[0].ToString();
            this.Fields = new Dictionary<string, object>() 
            { 
                  { "VisitDateTime", ((DateTime)row[6]).Ticks }
                , { "uid_ProviderOrganization", row[3].ToString().ToLower() }
                , { "uid_Patient", row[4].ToString().ToLower() }
                , { "uid_ProviderLocation", row[5].ToString().ToLower() }
            };
        }
    }
}