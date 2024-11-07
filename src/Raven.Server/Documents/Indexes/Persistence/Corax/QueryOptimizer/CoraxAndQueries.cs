using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public sealed class CoraxAndQueries : CoraxBooleanQueryBase
{
    private readonly List<CoraxBooleanItem> _queryStack;

    public CoraxAndQueries(IndexSearcher indexSearcher, CoraxQueryBuilder.Parameters parameters, CoraxBooleanItem left, CoraxBooleanItem right) :
        base(indexSearcher, parameters)
    {
        _queryStack = new List<CoraxBooleanItem>() {left, right};
    }

    public bool TryMerge(CoraxAndQueries other)
    {
        if (EqualsScoreFunctions(other) == false)
            return false;

        _queryStack.AddRange(other._queryStack);
        return true;
    }

    public bool TryAnd(IQueryMatch item)
    {
        switch (item)
        {
            case CoraxBooleanQueryBase cbqb:
                throw new InvalidOperationException($"CoraxBooleanQueryBase should be merged via {nameof(TryMerge)} method.");
            case CoraxBooleanItem cbi:
                _queryStack.Add(cbi);
                return true;
            default:
                return false;
        }
    }

    public override IQueryMatch Materialize()
    {
        var stack = CollectionsMarshal.AsSpan(_queryStack);
        var noStreaming = new CoraxQueryBuilder.StreamingOptimization();

        IQueryMatch match = null;
        stack.Sort(PrioritizeSort);
        //stack.Reverse(); // we want to have BIGGEST at the very beginning to avoid filling big match multiple times

        foreach (ref var query in stack)
        {
            var materializedQuery = query.Materialize(ref noStreaming);

            match = match is null
                ? materializedQuery
                : IndexSearcher.And(materializedQuery, match);
        }

        return IsBoosting ? IndexSearcher.Boost(match, Boosting.Value) : match;
    }

    private static int PrioritizeSort(CoraxBooleanItem firstUnaryItem, CoraxBooleanItem secondUnaryItem)
    {
        switch (firstUnaryItem.Operation)
        {
            //After benchmarks we discover it's not better to call termmatch as first item in case when MultiTermMatch has more terms than our termmmatch's posting lists has items;
            case UnaryMatchOperation.Equals when secondUnaryItem.Operation is not (UnaryMatchOperation.NotEquals or UnaryMatchOperation.Equals):
                return firstUnaryItem.Count.CompareTo(secondUnaryItem.Count);
            case UnaryMatchOperation.Equals when secondUnaryItem.Operation != UnaryMatchOperation.Equals:
                return -1;
        }

        if (firstUnaryItem.Operation != UnaryMatchOperation.Equals && secondUnaryItem.Operation == UnaryMatchOperation.Equals)
            return 1;
        if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation != UnaryMatchOperation.Between)
            return -1;
        if (firstUnaryItem.Operation != UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
            return 1;

        //This And(MultiTermMatch, MultiTermMatch) we force match with biggest amount of term in it to avoid crawling through
        if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
            return secondUnaryItem.Count.CompareTo(firstUnaryItem.Count);

        return secondUnaryItem.Count.CompareTo(firstUnaryItem.Count);
    }

    public new bool IsBoosting => Boosting.HasValue;
}
