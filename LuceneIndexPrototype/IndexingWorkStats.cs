﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LuceneIndexPrototype
{
    public class IndexingWorkStats
    {
        public int IndexingAttempts;
        public int IndexingSuccesses;
        public int IndexingErrors;

        public int ReduceAttempts;
        public int ReduceSuccesses;
        public int ReduceErrors;

        public Status Operation;

        public enum Status
        {
            Map,
            Reduce,
            Ignore
        }
    }
}
