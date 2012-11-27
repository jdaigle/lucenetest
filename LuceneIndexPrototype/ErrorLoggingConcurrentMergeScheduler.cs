using System;
using log4net;
using Lucene.Net.Index;

namespace LuceneIndexPrototype
{
    public class ErrorLoggingConcurrentMergeScheduler : ConcurrentMergeScheduler
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(ErrorLoggingConcurrentMergeScheduler));

        protected override void HandleMergeException(System.Exception exc)
        {
            try
            {
                base.HandleMergeException(exc);
            }
            catch (Exception e)
            {
                log.Warn("Concurrent merge failed", e);
            }
        }
    }
}
