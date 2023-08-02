﻿using System;
using Raven.Client.Util;

namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class QueueSinkPerformanceStats
{
    public QueueSinkPerformanceStats(TimeSpan duration)
    {
        DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
    }

    public int Id { get; set; }

    public DateTime Started { get; set; }

    public DateTime? Completed { get; set; }

    public double DurationInMs { get; }

    public QueueSinkPerformanceOperation Details { get; set; }

    public int NumberOfConsumedMessages{ get; set; }

    public int NumberOfProcessedMessages { get; set; }

    public Size CurrentlyAllocated { get; set; }

    public Size BatchSize { get; set; }

    public string BatchTransformationCompleteReason { get; set; }

    public string BatchStopReason { get; set; }

    public int ScriptErrorCount { get; set; }

    public bool? SuccessfullyProcessed { get; set; }
}
