﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Sorting;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Exceptions;
using Raven.Server.Indexing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public sealed class IndexReadOperation : IndexOperationBase
    {
        private static readonly string[] IntersectSeparators = { Constants.Documents.Querying.IntersectSeparator };

        private static readonly CompareInfo InvariantCompare = CultureInfo.InvariantCulture.CompareInfo;

        private readonly IndexType _indexType;
        private readonly bool _indexHasBoostedFields;

        private readonly IndexSearcher _searcher;
        private readonly RavenPerFieldAnalyzerWrapper _analyzer;
        private readonly IDisposable _releaseSearcher;
        private readonly IDisposable _releaseReadTransaction;
        private readonly int _maxNumberOfOutputsPerDocument;

        private readonly IState _state;

        public IndexReadOperation(Index index, LuceneVoronDirectory directory, IndexSearcherHolder searcherHolder, Transaction readTransaction)
            : base(index.Name, LoggingSource.Instance.GetLogger<IndexReadOperation>(index._indexStorage.DocumentDatabase.Name))
        {
            try
            {
                _analyzer = CreateAnalyzer(() => new LowerCaseKeywordAnalyzer(), index.Definition.MapFields, forQuerying: true);
            }
            catch (Exception e)
            {
                throw new IndexAnalyzerException(e);
            }

            _maxNumberOfOutputsPerDocument = index.MaxNumberOfOutputsPerDocument;
            _indexType = index.Type;
            _indexHasBoostedFields = index.HasBoostedFields;
            _releaseReadTransaction = directory.SetTransaction(readTransaction, out _state);
            _releaseSearcher = searcherHolder.GetSearcher(readTransaction, _state, out _searcher);
        }

        public int EntriesCount()
        {
            return _searcher.IndexReader.NumDocs();
        }

        public IEnumerable<Document> Query(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, CancellationToken token)
        {
            var pageSize = GetPageSize(_searcher, query.PageSize);
            var docsToGet = pageSize;
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(query.Parsed, query.Fields.Where, _analyzer);
            var sort = GetSort(query);
            var returnedResults = 0;

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
            {
                while (true)
                {
                    token.ThrowIfCancellationRequested();

                    var search = ExecuteQuery(luceneQuery, query.Start, docsToGet, sort);

                    totalResults.Value = search.TotalHits;

                    scope.RecordAlreadyPagedItemsInPreviousPage(search);

                    for (; position < search.ScoreDocs.Length && pageSize > 0; position++)
                    {
                        token.ThrowIfCancellationRequested();

                        var scoreDoc = search.ScoreDocs[position];
                        var document = _searcher.Doc(scoreDoc.Doc, _state);

                        string key;
                        if (retriever.TryGetKey(document, _state, out key) && scope.WillProbablyIncludeInResults(key) == false)
                        {
                            skippedResults.Value++;
                            continue;
                        }

                        var result = retriever.Get(document, scoreDoc.Score, _state);
                        if (scope.TryIncludeInResults(result) == false)
                        {
                            skippedResults.Value++;
                            continue;
                        }

                        returnedResults++;
                        yield return result;

                        if (returnedResults == pageSize)
                            yield break;
                    }

                    docsToGet += GetPageSize(_searcher, (long)(pageSize - returnedResults) * _maxNumberOfOutputsPerDocument);
                    if (search.TotalHits == search.ScoreDocs.Length)
                        break;

                    if (returnedResults >= pageSize)
                        break;
                }
            }
        }

        public IEnumerable<Document> IntersectQuery(IndexQueryServerSide query, FieldsToFetch fieldsToFetch, Reference<int> totalResults, Reference<int> skippedResults, IQueryResultRetriever retriever, CancellationToken token)
        {
            var subQueries = query.Query.Split(IntersectSeparators, StringSplitOptions.RemoveEmptyEntries);
            if (subQueries.Length <= 1)
                throw new InvalidOperationException("Invalid INTERSECT query, must have multiple intersect clauses.");

            //Not sure how to select the page size here??? The problem is that only docs in this search can be part 
            //of the final result because we're doing an intersection query (but we might exclude some of them)
            var pageSize = GetPageSize(_searcher, query.PageSize);
            int pageSizeBestGuess = GetPageSize(_searcher, ((long)query.Start + query.PageSize) * 2);
            int intersectMatches, skippedResultsInCurrentLoop = 0;
            int previousBaseQueryMatches = 0, currentBaseQueryMatches;

            var firstSubDocumentQuery = GetLuceneQuery(subQueries[0], query.DefaultOperator, query.DefaultField, _analyzer);
            var sort = GetSort(query);

            using (var scope = new IndexQueryingScope(_indexType, query, fieldsToFetch, _searcher, retriever, _state))
            {
                //Do the first sub-query in the normal way, so that sorting, filtering etc is accounted for
                var search = ExecuteQuery(firstSubDocumentQuery, 0, pageSizeBestGuess, sort);
                currentBaseQueryMatches = search.ScoreDocs.Length;
                var intersectionCollector = new IntersectionCollector(_searcher, search.ScoreDocs, _state);

                do
                {
                    token.ThrowIfCancellationRequested();
                    if (skippedResultsInCurrentLoop > 0)
                    {
                        // We get here because out first attempt didn't get enough docs (after INTERSECTION was calculated)
                        pageSizeBestGuess = pageSizeBestGuess * 2;

                        search = ExecuteQuery(firstSubDocumentQuery, 0, pageSizeBestGuess, sort);
                        previousBaseQueryMatches = currentBaseQueryMatches;
                        currentBaseQueryMatches = search.ScoreDocs.Length;
                        intersectionCollector = new IntersectionCollector(_searcher, search.ScoreDocs, _state);
                    }

                    for (var i = 1; i < subQueries.Length; i++)
                    {
                        var luceneSubQuery = GetLuceneQuery(subQueries[i], query.DefaultOperator, query.DefaultField, _analyzer);
                        _searcher.Search(luceneSubQuery, null, intersectionCollector, _state);
                    }

                    var currentIntersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                    intersectMatches = currentIntersectResults.Count;
                    skippedResultsInCurrentLoop = pageSizeBestGuess - intersectMatches;
                } while (intersectMatches < pageSize                      //stop if we've got enough results to satisfy the pageSize
                    && currentBaseQueryMatches < search.TotalHits           //stop if increasing the page size wouldn't make any difference
                    && previousBaseQueryMatches < currentBaseQueryMatches); //stop if increasing the page size didn't result in any more "base query" results

                var intersectResults = intersectionCollector.DocumentsIdsForCount(subQueries.Length).ToList();
                //It's hard to know what to do here, the TotalHits from the base search isn't really the TotalSize, 
                //because it's before the INTERSECTION has been applied, so only some of those results make it out.
                //Trying to give an accurate answer is going to be too costly, so we aren't going to try.
                totalResults.Value = search.TotalHits;
                skippedResults.Value = skippedResultsInCurrentLoop;

                //Using the final set of results in the intersectionCollector
                int returnedResults = 0;
                for (int i = query.Start; i < intersectResults.Count && (i - query.Start) < pageSizeBestGuess; i++)
                {
                    var indexResult = intersectResults[i];
                    var document = _searcher.Doc(indexResult.LuceneId, _state);

                    string key;
                    if (retriever.TryGetKey(document, _state, out key) && scope.WillProbablyIncludeInResults(key) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    var result = retriever.Get(document, indexResult.Score, _state);
                    if (scope.TryIncludeInResults(result) == false)
                    {
                        skippedResults.Value++;
                        skippedResultsInCurrentLoop++;
                        continue;
                    }

                    returnedResults++;
                    yield return result;
                    if (returnedResults == pageSize)
                        yield break;
                }
            }
        }

        private TopDocs ExecuteQuery(Query documentQuery, int start, int pageSize, Sort sort)
        {
            if (sort == null && _indexHasBoostedFields == false && IsBoostedQuery(documentQuery) == false)
            {
                if (pageSize == int.MaxValue || pageSize == _searcher.MaxDoc) // we want all docs, no sorting required
                {
                    var gatherAllCollector = new GatherAllCollector();
                    _searcher.Search(documentQuery, gatherAllCollector, _state);
                    return gatherAllCollector.ToTopDocs();
                }

                var noSortingCollector = new NonSortingCollector(Math.Abs(pageSize + start));

                _searcher.Search(documentQuery, noSortingCollector, _state);
                
                return noSortingCollector.ToTopDocs();
            }

            var minPageSize = GetPageSize(_searcher, (long)pageSize + start);

            if (sort != null)
            {
                _searcher.SetDefaultFieldSortScoring(true, false);
                try
                {
                    return _searcher.Search(documentQuery, null, minPageSize, sort, _state);
                }
                finally
                {
                    _searcher.SetDefaultFieldSortScoring(false, false);
                }
            }

            return _searcher.Search(documentQuery, null, minPageSize, _state);
        }

        private static bool IsBoostedQuery(Query query)
        {
            if (query.Boost > 1)
                return true;

            BooleanQuery booleanQuery = query as BooleanQuery;

            if (booleanQuery == null)
                return false;

            foreach (var clause in booleanQuery.Clauses)
            {
                if (clause.Query.Boost > 1)
                    return true;
            }

            return false;
        }
        
        private Sort GetSort(IndexQueryServerSide query)
        {
            var orderByFields = query.Fields.OrderBy;

            if (orderByFields == null)
                return null;

            var sort = new List<SortField>();

            foreach (var field in orderByFields)
            {
                var sortOptions = SortOptions.String;

                if (field.Name == Constants.Documents.Indexing.Fields.IndexFieldScoreName)
                {
                    sort.Add(SortField.FIELD_SCORE);
                    continue;
                }

                if (InvariantCompare.IsPrefix(field.Name, Constants.Documents.Indexing.Fields.AlphaNumericFieldName, CompareOptions.None))
                {
                    var customFieldName = SortFieldHelper.ExtractName(field.Name);
                    if (customFieldName.IsNullOrWhiteSpace())
                        throw new InvalidOperationException("Alphanumeric sort: cannot figure out what field to sort on!");

                    var anSort = new AlphaNumericComparatorSource();
                    sort.Add(new SortField(customFieldName, anSort, field.Ascending == false));
                    continue;
                }

                if (InvariantCompare.IsPrefix(field.Name, Constants.Documents.Indexing.Fields.RandomFieldName, CompareOptions.None))
                {
                    var customFieldName = SortFieldHelper.ExtractName(field.Name);
                    if (customFieldName.IsNullOrWhiteSpace()) // truly random
                        sort.Add(new RandomSortField(Guid.NewGuid().ToString()));
                    else
                        sort.Add(new RandomSortField(customFieldName));
                    
                    continue;
                }

                switch (field.OrderingType)
                {
                    case Queries.Parser.OrderByFieldType.Long:
                    case Queries.Parser.OrderByFieldType.Double:
                        sortOptions = SortOptions.Numeric;
                        break;
                }

                sort.Add(new SortField(IndexField.ReplaceInvalidCharactersInFieldName(field.Name), (int)sortOptions, field.Ascending == false));
            }

            return new Sort(sort.ToArray());
        }

        public HashSet<string> Terms(string field, string fromValue, int pageSize, CancellationToken token)
        {
            var results = new HashSet<string>();
            using (var termEnum = _searcher.IndexReader.Terms(new Term(field, fromValue ?? string.Empty), _state))
            {
                if (string.IsNullOrEmpty(fromValue) == false) // need to skip this value
                {
                    while (termEnum.Term == null || fromValue.Equals(termEnum.Term.Text))
                    {
                        token.ThrowIfCancellationRequested();

                        if (termEnum.Next(_state) == false)
                            return results;
                    }
                }
                while (termEnum.Term == null ||
                    field.Equals(termEnum.Term.Field))
                {
                    token.ThrowIfCancellationRequested();

                    if (termEnum.Term != null)
                        results.Add(termEnum.Term.Text);

                    if (results.Count >= pageSize)
                        break;

                    if (termEnum.Next(_state) == false)
                        break;
                }
            }

            return results;
        }

        public IEnumerable<Document> MoreLikeThis(MoreLikeThisQueryServerSide query, HashSet<string> stopWords, Func<string[], IQueryResultRetriever> createRetriever, CancellationToken token)
        {
            var documentQuery = new BooleanQuery();

            if (string.IsNullOrWhiteSpace(query.DocumentId) == false)
                documentQuery.Add(new TermQuery(new Term(Constants.Documents.Indexing.Fields.DocumentIdFieldName, query.DocumentId.ToLowerInvariant())), Occur.MUST);

            foreach (var key in query.MapGroupFields.Keys)
                documentQuery.Add(new TermQuery(new Term(key, query.MapGroupFields[key])), Occur.MUST);

            var td = _searcher.Search(documentQuery, 1, _state);

            // get the current Lucene docid for the given RavenDB doc ID
            if (td.ScoreDocs.Length == 0)
                throw new InvalidOperationException("Document " + query.DocumentId + " could not be found");

            var ir = _searcher.IndexReader;
            var mlt = new RavenMoreLikeThis(ir, query, _state);

            if (stopWords != null)
                mlt.SetStopWords(stopWords);

            var fieldNames = query.Fields ?? ir.GetFieldNames(IndexReader.FieldOption.INDEXED)
                                    .Where(x => x != Constants.Documents.Indexing.Fields.DocumentIdFieldName && x != Constants.Documents.Indexing.Fields.ReduceKeyFieldName)
                                    .ToArray();

            mlt.SetFieldNames(fieldNames);
            mlt.Analyzer = _analyzer;

            var pageSize = GetPageSize(_searcher, query.PageSize);
            var mltQuery = mlt.Like(td.ScoreDocs[0].Doc);
            var tsdc = TopScoreDocCollector.Create(pageSize, true);

            if (string.IsNullOrWhiteSpace(query.AdditionalQuery) == false)
            {
                var additionalQuery = QueryBuilder.BuildQuery(query.AdditionalQuery, _analyzer);
                mltQuery = new BooleanQuery
                    {
                        {mltQuery, Occur.MUST},
                        {additionalQuery, Occur.MUST},
                    };
            }

            _searcher.Search(mltQuery, tsdc, _state);
            var hits = tsdc.TopDocs().ScoreDocs;
            var baseDocId = td.ScoreDocs[0].Doc;

            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var fieldsToFetch = string.IsNullOrWhiteSpace(query.DocumentId)
                ? _searcher.Doc(baseDocId, _state).GetFields().Cast<AbstractField>().Select(x => x.Name).Distinct().ToArray()
                : null;

            var retriever = createRetriever(fieldsToFetch);

            foreach (var hit in hits)
            {
                if (hit.Doc == baseDocId)
                    continue;

                var doc = _searcher.Doc(hit.Doc, _state);
                var id = doc.Get(Constants.Documents.Indexing.Fields.DocumentIdFieldName, _state) ?? doc.Get(Constants.Documents.Indexing.Fields.ReduceKeyFieldName, _state);
                if (id == null)
                    continue;

                if (ids.Add(id) == false)
                    continue;

                yield return retriever.Get(doc, hit.Score, _state);
            }
        }

        public IEnumerable<BlittableJsonReaderObject> IndexEntries(IndexQueryServerSide query, Reference<int> totalResults, DocumentsOperationContext documentsContext, CancellationToken token)
        {
            var docsToGet = GetPageSize(_searcher, query.PageSize);
            var position = query.Start;

            var luceneQuery = GetLuceneQuery(query.Query, query.DefaultOperator, query.DefaultField, _analyzer);
            var sort = GetSort(query);

            var search = ExecuteQuery(luceneQuery, query.Start, docsToGet, sort);
            var termsDocs = IndexedTerms.ReadAllEntriesFromIndex(_searcher.IndexReader, documentsContext, _state);

            totalResults.Value = search.TotalHits;

            for (var index = position; index < search.ScoreDocs.Length; index++)
            {
                token.ThrowIfCancellationRequested();

                var scoreDoc = search.ScoreDocs[index];
                var document = termsDocs[scoreDoc.Doc];

                yield return document;
            }
        }

        public override void Dispose()
        {
            _analyzer?.Dispose();
            _releaseSearcher?.Dispose();
            _releaseReadTransaction?.Dispose();
        }
    }
}