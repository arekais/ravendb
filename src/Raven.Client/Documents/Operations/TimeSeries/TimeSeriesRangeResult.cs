﻿using System;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public class TimeSeriesRangeResult
    {
        public DateTime From, To;
        public TimeSeriesEntry[] Entries;
        public long? TotalResults;
        internal string Hash;
    }
}
