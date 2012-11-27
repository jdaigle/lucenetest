using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using log4net;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace LuceneIndexPrototype
{
    public class Index : IDisposable
    {
        protected static readonly ILog logIndexing = LogManager.GetLogger(typeof(Index).FullName + ".Indexing");
        protected static readonly ILog logQuerying = LogManager.GetLogger(typeof(Index).FullName + ".Querying");

        public Index(Directory directory, IndexDefinition indexDefinition)
        {
            if (directory == null) throw new ArgumentNullException("directory");
            if (indexDefinition == null) throw new ArgumentNullException("indexDefinition");

            this.name = indexDefinition.Name;
            this.indexDefinition = indexDefinition;
            this.indexDefinitionFieldHelper = new IndexDefinitionFieldHelper(this.indexDefinition);
            logIndexing.DebugFormat("Creating index for {0}", name);
            this.directory = directory;

            RecreateSearcher();
        }

        protected readonly IndexDefinition indexDefinition;
        protected readonly IndexDefinitionFieldHelper indexDefinitionFieldHelper;
        private readonly string name;
        private Directory directory;
        private IndexWriter indexWriter;
        private readonly object writeLock = new object();
        private volatile string waitReason;
        private volatile bool disposed;
        private int docCountSinceLastOptimization;

        private void RecreateSearcher()
        {
            //if (indexWriter == null)
            //{
            //    currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(directory, true));
            //}
            //else
            //{
            //    var indexReader = indexWriter.GetReader();
            //    currentIndexSearcherHolder.SetIndexSearcher(new IndexSearcher(indexReader));
            //}
        }

        public void Dispose()
        {
            try
            {
                // this is here so we can give good logs in the case of a long shutdown process
                if (Monitor.TryEnter(writeLock, 100) == false)
                {
                    var localReason = waitReason;
                    if (localReason != null)
                        logIndexing.WarnFormat("Waiting for {0} to complete before disposing of index {1}, that might take a while if the server is very busy",
                         localReason, name);

                    Monitor.Enter(writeLock);
                }

                disposed = true;
                //if (currentIndexSearcherHolder != null)
                //{
                //    var item = currentIndexSearcherHolder.SetIndexSearcher(null);
                //    if (item.WaitOne(TimeSpan.FromSeconds(5)) == false)
                //    {
                //        logIndexing.Warn("After closing the index searching, we waited for 5 seconds for the searching to be done, but it wasn't. Continuing with normal shutdown anyway.");
                //        Console.Beep();
                //    }
                //}

                if (indexWriter != null)
                {
                    var writer = indexWriter;
                    indexWriter = null;

                    try
                    {
                        writer.Analyzer.Close();
                    }
                    catch (Exception e)
                    {
                        logIndexing.Error("Error while closing the index (closing the analyzer failed)", e);
                    }

                    try
                    {
                        writer.Dispose();
                    }
                    catch (Exception e)
                    {
                        logIndexing.Error("Error when closing the index", e);
                    }
                }

                try
                {
                    directory.Dispose();
                }
                catch (Exception e)
                {
                    logIndexing.Error("Error when closing the directory", e);
                }
            }
            finally
            {
                Monitor.Exit(writeLock);
            }
        }

        public void Flush()
        {
            lock (writeLock)
            {
                if (disposed)
                    return;
                if (indexWriter == null)
                    return;

                try
                {

                    waitReason = "Flush";
                    indexWriter.Commit();
                }
                finally
                {
                    waitReason = null;
                }
            }
        }

        public void MergeSegments()
        {
            //if (docCountSinceLastOptimization <= 2048) return;
            lock (writeLock)
            {
                waitReason = "Merge / Optimize";
                try
                {
                    if (indexWriter == null)
                    {
                        indexWriter = CreateIndexWriter(directory);
                    }
                    indexWriter.Optimize();
                }
                finally
                {
                    waitReason = null;
                }
                docCountSinceLastOptimization = 0;
            }
        }

        public void IndexDocuments(IEnumerable<IndexableDocument> documents)
        {
            var count = 0;
            var sourceCount = 0;
            Write(null, (indexWriter, analyzer, stats) =>
            {
                var docIdTerm = new Term("__document_id");
                var luceneDoc = new Document();
                var documentIdField = new Field("__document_id", "dummy", Field.Store.YES,
                                                Field.Index.NOT_ANALYZED_NO_NORMS);
                foreach (var doc in documents)
                {
                    Interlocked.Increment(ref sourceCount);
                    Interlocked.Increment(ref count);
                    //indexWriter.DeleteDocuments(docIdTerm.CreateTerm(doc.DocumentId.ToLowerInvariant()));
                    luceneDoc.GetFields().Clear();
                    documentIdField.SetValue(doc.DocumentId.ToLowerInvariant());
                    luceneDoc.Add(documentIdField);
                    foreach (var field in doc.Fields.SelectMany(x => this.indexDefinitionFieldHelper.CreateFields(x.Key, x.Value)))
                    {
                        luceneDoc.Add(field);
                    }
                    LogIndexedDocument(doc.DocumentId, luceneDoc);
                    //indexWriter.AddDocument(luceneDoc, analyzer);
                    indexWriter.UpdateDocument(docIdTerm.CreateTerm(doc.DocumentId.ToLowerInvariant()), luceneDoc, analyzer);
                    Interlocked.Increment(ref stats.IndexingSuccesses);
                }
                return sourceCount;
            });
            //AddindexingPerformanceStat(new IndexingPerformanceStats
            //{
            //    OutputCount = count,
            //    InputCount = sourceCount,
            //    Duration = sw.Elapsed,
            //    Operation = "Index",
            //    Started = start
            //});
            logIndexing.DebugFormat("Indexed {0} documents for {1}", count, name);
        }

        public DateTime LastIndexTime { get; set; }

        protected void Write(WorkContext context, Func<IndexWriter, Analyzer, IndexingWorkStats, int> action)
        {
            if (disposed)
                throw new ObjectDisposedException("Index " + name + " has been disposed");
            LastIndexTime = DateTime.UtcNow;
            lock (writeLock)
            {
                bool shouldRecreateSearcher;
                var toDispose = new List<Action>();
                Analyzer searchAnalyzer = null;
                try
                {
                    waitReason = "Write";
                    try
                    {
                        searchAnalyzer = CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose);
                    }
                    catch (Exception e)
                    {
                        //context.AddError(name, "Creating Analyzer", e.ToString());
                        throw;
                    }

                    if (indexWriter == null)
                    {
                        indexWriter = CreateIndexWriter(directory);
                    }

                    var locker = directory.MakeLock("writing-to-index.lock");
                    try
                    {
                        var stats = new IndexingWorkStats();
                        try
                        {
                            var changedDocs = action(indexWriter, searchAnalyzer, stats);
                            docCountSinceLastOptimization += changedDocs;
                            shouldRecreateSearcher = changedDocs > 0;
                            //foreach (IIndexExtension indexExtension in indexExtensions.Values)
                            //{
                            //    indexExtension.OnDocumentsIndexed(currentlyIndexDocuments);
                            //}
                        }
                        catch (Exception e)
                        {
                            //context.AddError(name, null, e.ToString());
                            throw;
                        }

                        UpdateIndexingStats(context, stats);
                        Flush(); // just make sure changes are flushed to disk
                    }
                    finally
                    {
                        locker.Release();
                    }
                }
                finally
                {
                    //currentlyIndexDocuments.Clear();
                    if (searchAnalyzer != null)
                        searchAnalyzer.Close();
                    foreach (Action dispose in toDispose)
                    {
                        dispose();
                    }
                    waitReason = null;
                    LastIndexTime = DateTime.UtcNow;
                }
                if (shouldRecreateSearcher)
                    RecreateSearcher();
            }
        }

        protected void LogIndexedDocument(string key, Document luceneDoc)
        {
            if (logIndexing.IsDebugEnabled)
            {
                var fieldsForLogging = luceneDoc.GetFields().Cast<IFieldable>().Select(x => new
                {
                    Name = x.Name,
                    Value = x.IsBinary ? "<binary>" : x.StringValue,
                    Indexed = x.IsIndexed,
                    Stored = x.IsStored,
                });
                var sb = new StringBuilder();
                foreach (var fieldForLogging in fieldsForLogging)
                {
                    sb.Append("\t").Append(fieldForLogging.Name)
                        .Append(" ")
                        .Append(fieldForLogging.Indexed ? "I" : "-")
                        .Append(fieldForLogging.Stored ? "S" : "-")
                        .Append(": ")
                        .Append(fieldForLogging.Value)
                        .AppendLine();
                }

                logIndexing.DebugFormat("Indexing on {0} result in index {1} gave document: {2}", key, name,
                                sb.ToString());
            }


        }

        protected void UpdateIndexingStats(WorkContext context, IndexingWorkStats stats)
        {
        }

        public PerFieldAnalyzerWrapper CreateAnalyzer(Analyzer defaultAnalyzer, ICollection<Action> toDispose, bool forQuerying = false)
        {
            toDispose.Add(defaultAnalyzer.Close);

            //string value;
            //if (indexDefinition.Analyzers.TryGetValue(Constants.AllFields, out value))
            //{
            //    defaultAnalyzer = IndexingExtensions.CreateAnalyzerInstance(Constants.AllFields, value);
            //    toDispose.Add(defaultAnalyzer.Close);
            //}
            var perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer);
            foreach (var analyzer in indexDefinition.Analyzers)
            {
                //Analyzer analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(analyzer.Key, analyzer.Value);
                //toDispose.Add(analyzerInstance.Close);

                //if (forQuerying)
                //{
                //    var customAttributes = analyzerInstance.GetType().GetCustomAttributes(typeof(NotForQueryingAttribute), false);
                //    if (customAttributes.Length > 0)
                //        continue;
                //}

                //perFieldAnalyzerWrapper.AddAnalyzer(analyzer.Key, analyzerInstance);
            }
            StandardAnalyzer standardAnalyzer = null;
            KeywordAnalyzer keywordAnalyzer = null;
            foreach (var fieldIndexing in indexDefinition.Indexes)
            {
                switch (fieldIndexing.Value)
                {
                    case FieldIndexing.NotAnalyzed:
                        if (keywordAnalyzer == null)
                        {
                            keywordAnalyzer = new KeywordAnalyzer();
                            toDispose.Add(keywordAnalyzer.Close);
                        }
                        perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, keywordAnalyzer);
                        break;
                    case FieldIndexing.Analyzed:
                        if (indexDefinition.Analyzers.ContainsKey(fieldIndexing.Key))
                        {
                            continue; // already added
                        }
                        if (standardAnalyzer == null)
                        {
                            standardAnalyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30);
                            toDispose.Add(standardAnalyzer.Close);
                        }
                        perFieldAnalyzerWrapper.AddAnalyzer(fieldIndexing.Key, standardAnalyzer);
                        break;
                }
            }
            return perFieldAnalyzerWrapper;
        }

        private static IndexWriter CreateIndexWriter(Directory directory)
        {
            var indexWriter = new IndexWriter(directory, new StopAnalyzer(Lucene.Net.Util.Version.LUCENE_30), IndexWriter.MaxFieldLength.UNLIMITED);
            using (indexWriter.MergeScheduler) { }
            indexWriter.SetMergeScheduler(new ErrorLoggingConcurrentMergeScheduler());

            // RavenDB already manages the memory for those, no need for Lucene to do this as well
            indexWriter.MergeFactor = 1024;
            indexWriter.SetMaxBufferedDocs(IndexWriter.DISABLE_AUTO_FLUSH);
            indexWriter.SetRAMBufferSizeMB(1024);
            return indexWriter;
        }

    }
}
