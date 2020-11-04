﻿using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries.Graph
{
    public class QueryQueryStepGatherer : QueryPlanVisitor
    {
        public readonly List<(QueryQueryStep QueryStep, bool IsIndexQuery)> QuerySteps = new List<(QueryQueryStep QueryStep, bool IsIndexQuery)>();
        public override Task VisitQueryQueryStepAsync(QueryQueryStep qqs)
        {
            var indexName = qqs.GetIndexName;
            QuerySteps.Add((qqs, !string.IsNullOrWhiteSpace(indexName)));

            return Task.CompletedTask;
        }
    }
}
