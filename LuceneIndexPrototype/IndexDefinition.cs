using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lucene.Net.Documents;

namespace LuceneIndexPrototype
{
    public class IndexDefinition
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IndexDefinition"/> class.
        /// </summary>
        public IndexDefinition()
        {
            Indexes = new Dictionary<string, FieldIndexing>();
            Stores = new Dictionary<string, FieldStorage>();
            Analyzers = new Dictionary<string, string>();
            SortOptions = new Dictionary<string, SortOptions>();
        }

        /// <summary>
        /// Get or set the name of the index
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the stores options
        /// </summary>
        /// <value>The stores.</value>
        public IDictionary<string, FieldStorage> Stores { get; set; }

        /// <summary>
        /// Gets or sets the indexing options
        /// </summary>
        /// <value>The indexes.</value>
        public IDictionary<string, FieldIndexing> Indexes { get; set; }

        /// <summary>
        /// Gets or sets the sort options.
        /// </summary>
        /// <value>The sort options.</value>
        public IDictionary<string, SortOptions> SortOptions { get; set; }

        /// <summary>
        /// Gets or sets the analyzers options
        /// </summary>
        /// <value>The analyzers.</value>
        public IDictionary<string, string> Analyzers { get; set; }

        public Field.Index GetIndex(string name, Field.Index? defaultIndex)
        {
            var self = this;
            if (self.Indexes == null)
                return defaultIndex ?? Field.Index.ANALYZED_NO_NORMS;
            FieldIndexing value;
            if (self.Indexes.TryGetValue(name, out value) == false)
            {
                if (self.Indexes.TryGetValue("__all_fields", out value) == false)
                {
                    string ignored;
                    if (self.Analyzers.TryGetValue(name, out ignored) ||
                        self.Analyzers.TryGetValue("__all_fields", out ignored))
                    {
                        return Field.Index.ANALYZED; // if there is a custom analyzer, the value should be analyzed
                    }
                    return defaultIndex ?? Field.Index.ANALYZED_NO_NORMS;
                }
            }
            switch (value)
            {
                case FieldIndexing.No:
                    return Field.Index.NO;
                case FieldIndexing.Analyzed:
                    return Field.Index.ANALYZED_NO_NORMS;
                case FieldIndexing.NotAnalyzed:
                    return Field.Index.NOT_ANALYZED_NO_NORMS;
                case FieldIndexing.Default:
                    return defaultIndex ?? Field.Index.ANALYZED_NO_NORMS;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public Field.Store GetStorage(string name, Field.Store defaultStorage)
        {
            var self = this;
            if (self.Stores == null)
                return defaultStorage;
            FieldStorage value;
            if (self.Stores.TryGetValue(name, out value) == false)
            {
                // do we have a overriding default?
                if (self.Stores.TryGetValue("__all_fields", out value) == false)
                    return defaultStorage;
            }
            switch (value)
            {
                case FieldStorage.Yes:
                    return Field.Store.YES;
                case FieldStorage.No:
                    return Field.Store.NO;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public SortOptions? GetSortOption(string name)
        {
            var self = this;
            SortOptions value;
            if (self.SortOptions.TryGetValue(name, out value))
            {
                return value;
            }
            if (self.SortOptions.TryGetValue("__all_fields", out value))
                return value;

            if (name.EndsWith("_Range"))
            {
                string nameWithoutRange = name.Substring(0, name.Length - "_Range".Length);
                if (self.SortOptions.TryGetValue(nameWithoutRange, out value))
                    return value;

                if (self.SortOptions.TryGetValue("__all_fields", out value))
                    return value;
            }
            //if (CurrentOperationContext.Headers.Value == null)
            //    return value;

            //var hint = CurrentOperationContext.Headers.Value["SortHint-" + name];
            //if (string.IsNullOrEmpty(hint))
            //    return value;
            //Enum.TryParse(hint, true, out value);
            return value;
        }
    }
}
