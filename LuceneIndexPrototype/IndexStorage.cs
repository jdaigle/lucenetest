using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.ConstrainedExecution;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Directory = System.IO.Directory;

namespace LuceneIndexPrototype
{
    public class IndexStorage : CriticalFinalizerObject, IDisposable
    {
        private const string IndexVersion = "1.0";
        private readonly string path = @"c:\temp\indexstorage\";

        private readonly Analyzer dummyAnalyzer = new SimpleAnalyzer();
        private static readonly ILog log = LogManager.GetLogger(typeof(IndexStorage));
        private static readonly ILog startupLog = LogManager.GetLogger(typeof(IndexStorage).FullName + ".Startup");
        private bool resetIndexOnUncleanShutdown;
        private readonly FileStream crashMarker;
        private readonly ConcurrentDictionary<string, Index> indexes = new ConcurrentDictionary<string, Index>(StringComparer.InvariantCultureIgnoreCase);

        public IndexStorage()
        {
            try
            {
                if (Directory.Exists(path) == false)
                    Directory.CreateDirectory(path);

                var crashMarkerPath = Path.Combine(path, "indexing.crash-marker");

                if (File.Exists(crashMarkerPath))
                {
                    // the only way this can happen is if we crashed because of a power outage
                    // in this case, we consider all open indexes to be corrupt and force them
                    // to be reset. This is because to get better perf, we don't flush the files to disk,
                    // so in the case of a power outage, we can't be sure that there wasn't still stuff in
                    // the OS buffer that wasn't written yet.
                    resetIndexOnUncleanShutdown = true;
                }

                // The delete on close ensures that the only way this file will exists is if there was
                // a power outage while the server was running.
                crashMarker = File.Create(crashMarkerPath, 16, FileOptions.DeleteOnClose);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public Index OpenIndexOnStartup(IndexDefinition indexDefinition)
        {
            var indexName = indexDefinition.Name;
            startupLog.DebugFormat("Loading saved index {0}", indexDefinition.Name);

            Index indexImplementation;
            bool resetTried = false;
            while (true)
            {
                try
                {
                    var luceneDirectory = OpenOrCreateLuceneDirectory(indexDefinition, createIfMissing: resetTried);
                    indexImplementation = new Index(luceneDirectory, indexDefinition);
                    //documentDatabase.TransactionalStorage.Batch(accessor =>
                    //{
                    //    var read = accessor.Lists.Read("Raven/Indexes/QueryTime", indexName);
                    //    if (read == null)
                    //        return;

                    //    var dateTime = read.Data.Value<DateTime>("LastQueryTime");
                    //    indexImplementation.MarkQueried(dateTime);
                    //    if (dateTime > latestPersistedQueryTime)
                    //        latestPersistedQueryTime = dateTime;
                    //});
                    break;
                }
                catch (Exception e)
                {
                    // exception can be thrown if index needs to be reset :(
                    if (resetTried)
                        throw new InvalidOperationException("Could not open / create index" + indexName + ", reset already tried", e);
                    resetTried = true;
                    startupLog.Warn("Could not open index " + indexName + ", forcibly resetting index", e);
                    try
                    {
                        //documentDatabase.TransactionalStorage.Batch(accessor =>
                        //{
                        //    accessor.Indexing.DeleteIndex(indexName);
                        //    accessor.Indexing.AddIndex(indexName, indexDefinition.IsMapReduce);
                        //});

                        var indexDirectory = indexName;
                        var indexFullPath = Path.Combine(path, indexDirectory);
                        //IOExtensions.DeleteDirectory(indexFullPath);
                        if (Directory.Exists(indexFullPath))
                        {
                            Directory.Delete(indexFullPath, true);
                        }

                        var luceneDirectory = OpenOrCreateLuceneDirectory(indexDefinition, createIfMissing: resetTried);
                        indexImplementation = new Index(luceneDirectory, indexDefinition);
                    }
                    catch (Exception exception)
                    {
                        throw new InvalidOperationException("Could not reset index " + indexName, exception);
                    }
                }
            }
            indexes.TryAdd(indexName, indexImplementation);
            startupLog.WarnFormat("Index Created {0}", indexImplementation);
            return indexImplementation;
        }

        protected Lucene.Net.Store.Directory OpenOrCreateLuceneDirectory(
            IndexDefinition indexDefinition,
            string indexName = null,
            bool createIfMissing = true)
        {
            Lucene.Net.Store.Directory directory;
            var indexDirectory = indexName ?? FixupIndexName(indexDefinition.Name, path);
            var indexFullPath = Path.Combine(path, indexDirectory);
            directory = FSDirectory.Open(new DirectoryInfo(indexFullPath));

            if (!IndexReader.IndexExists(directory))
            {
                if (createIfMissing == false)
                    throw new InvalidOperationException("Index does not exists: " + indexDirectory);

                WriteIndexVersion(directory);

                //creating index structure if we need to
                new IndexWriter(directory, dummyAnalyzer, IndexWriter.MaxFieldLength.UNLIMITED).Dispose();
            }
            else
            {
                EnsureIndexVersionMatches(indexName, directory);
                if (directory.FileExists("write.lock"))// force lock release, because it was still open when we shut down
                {
                    IndexWriter.Unlock(directory);
                    // for some reason, just calling unlock doesn't remove this file
                    directory.DeleteFile("write.lock");
                }
                if (directory.FileExists("writing-to-index.lock")) // we had an unclean shutdown
                {
                    if (resetIndexOnUncleanShutdown)
                        throw new InvalidOperationException("Rude shutdown detected on: " + indexDirectory);

                    CheckIndexAndRecover(directory, indexDirectory);
                    directory.DeleteFile("writing-to-index.lock");
                }
            }

            return directory;
        }

        private static void CheckIndexAndRecover(Lucene.Net.Store.Directory directory, string indexDirectory)
        {
            startupLog.WarnFormat("Unclean shutdown detected on {0}, checking the index for errors. This may take a while.", indexDirectory);

            var memoryStream = new MemoryStream();
            var stringWriter = new StreamWriter(memoryStream);
            var checkIndex = new CheckIndex(directory);

            if (startupLog.IsWarnEnabled)
                checkIndex.SetInfoStream(stringWriter);

            var sp = Stopwatch.StartNew();
            var status = checkIndex.CheckIndex_Renamed_Method();
            sp.Stop();
            if (startupLog.IsWarnEnabled)
            {
                startupLog.WarnFormat("Checking index {0} took: {1}, clean: {2}", indexDirectory, sp.Elapsed, status.clean);
                memoryStream.Position = 0;

                log.Warn(new StreamReader(memoryStream).ReadToEnd());
            }

            if (status.clean)
                return;

            startupLog.WarnFormat("Attempting to fix index: {0}", indexDirectory);
            sp.Restart();
            checkIndex.FixIndex(status);
            startupLog.WarnFormat("Fixed index {0} in {1}", indexDirectory, sp.Elapsed);
        }

        private static void WriteIndexVersion(Lucene.Net.Store.Directory directory)
        {
            using (var indexOutput = directory.CreateOutput("index.version"))
            {
                indexOutput.WriteString(IndexVersion);
                indexOutput.Flush();
            }
        }

        private static void EnsureIndexVersionMatches(string indexName, Lucene.Net.Store.Directory directory)
        {
            if (directory.FileExists("index.version") == false)
            {
                throw new InvalidOperationException("Could not find index.version " + indexName + ", resetting index");
            }
            using (var indexInput = directory.OpenInput("index.version"))
            {
                var versionFromDisk = indexInput.ReadString();
                if (versionFromDisk != IndexVersion)
                    throw new InvalidOperationException("Index " + indexName + " is of version " + versionFromDisk +
                                                        " which is not compatible with " + IndexVersion + ", resetting index");
            }
        }

        public static string FixupIndexName(string index, string path)
        {
            index = index.Trim();
            string prefix = null;
            if (path.Length + index.Length > 230 ||
                Encoding.Unicode.GetByteCount(index) >= 255)
            {
                using (var md5 = MD5.Create())
                {
                    var bytes = md5.ComputeHash(Encoding.UTF8.GetBytes(index));
                    return prefix + Convert.ToBase64String(bytes);
                }
            }
            return index;
        }

        public void Dispose()
        {
            foreach (var item in indexes.Values)
            {
                item.Dispose();
            }
            //var exceptionAggregator = new ExceptionAggregator(log, "Could not properly close index storage");

            //exceptionAggregator.Execute(() => Parallel.ForEach(indexes.Values, index => exceptionAggregator.Execute(index.Dispose)));

            //exceptionAggregator.Execute(() => dummyAnalyzer.Close());

            //exceptionAggregator.Execute(() =>
            //{
            //    if (crashMarker != null)
            //        crashMarker.Dispose();
            //});

            //exceptionAggregator.ThrowIfNeeded();
        }
    }
}
