﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Jint.Native;
using Jint.Native.Object;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using BlittableJsonTextWriterExtensions = Sparrow.Json.BlittableJsonTextWriterExtensions;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers
{
    public class IndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/replace", "POST", AuthorizationStatus.ValidUser)]
        public Task Replace()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + name;

            var oldIndex = Database.IndexStore.GetIndex(name);
            var newIndex = Database.IndexStore.GetIndex(replacementName);

            if (oldIndex == null && newIndex == null)
                throw new IndexDoesNotExistException($"Could not find '{name}' and '{replacementName}' indexes.");

            if (newIndex == null)
                throw new IndexDoesNotExistException($"Could not find side-by-side index for '{name}'.");

            while (Database.DatabaseShutdown.IsCancellationRequested == false)
            {
                if (Database.IndexStore.TryReplaceIndexes(name, newIndex.Name, Database.DatabaseShutdown))
                    break;
            }

            return NoContent();
        }

        [RavenAction("/databases/*/indexes/source", "GET", AuthorizationStatus.ValidUser)]
        public async Task Source()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (index.Type.IsStatic() == false)
                throw new InvalidOperationException("Source can be only retrieved for static indexes.");

            string source = null;
            switch (index.Type)
            {
                case IndexType.Map:
                    var staticMapIndex = (MapIndex)index;
                    source = staticMapIndex._compiled.Source;
                    break;

                case IndexType.MapReduce:
                    var staticMapReduceIndex = (MapReduceIndex)index;
                    source = staticMapReduceIndex._compiled.Source;
                    break;
            }

            if (string.IsNullOrWhiteSpace(source))
                throw new InvalidOperationException("Could not retrieve source for given index.");

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await context.WriteAsync(writer, new DynamicJsonValue
                {
                    ["Index"] = index.Name,
                    ["Source"] = source
                });
            }
        }

        [RavenAction("/databases/*/indexes/history", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetIndexHistory()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            List<IndexHistoryEntry> history;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(ctx, Database.Name))
            {
                var indexesHistory = rawRecord.IndexesHistory;
                if (indexesHistory.TryGetValue(name, out history) == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Index");
                await writer.WriteStringAsync(name);
                await writer.WriteCommaAsync();

                if (history == null || history.Count == 0)
                {
                    await writer.WriteStartArrayAsync();
                    await writer.WriteEndArrayAsync();
                    await writer.WriteEndObjectAsync();
                    return;
                }

                await writer.WriteArrayAsync(context, "History", history, async (w, c, entry) =>
                {
                    await w.WriteStartObjectAsync();

                    await w.WritePropertyNameAsync(nameof(IndexHistoryEntry.Definition));
                    await w.WriteIndexDefinition(c, entry.Definition);
                    await w.WriteCommaAsync();

                    await w.WritePropertyNameAsync(nameof(IndexHistoryEntry.CreatedAt));
                    await w.WriteDateTimeAsync(entry.CreatedAt, isUtc: true);
                    await w.WriteCommaAsync();

                    await w.WritePropertyNameAsync(nameof(IndexHistoryEntry.Source));
                    await w.WriteStringAsync(entry.Source);
                    await w.WriteEndObjectAsync();
                });

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes/has-changed", "POST", AuthorizationStatus.ValidUser)]
        public async Task HasChanged()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/definition"))
            {
                var indexDefinition = JsonDeserializationServer.IndexDefinition(json);

                if (indexDefinition?.Name == null || indexDefinition.Maps.Count == 0)
                    throw new BadRequestException("Index definition must contain name and at least one map.");

                var changed = Database.IndexStore.HasChanged(indexDefinition);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    await writer.WriteStartObjectAsync();
                    await writer.WritePropertyNameAsync("Changed");
                    await writer.WriteBoolAsync(changed);
                    await writer.WriteEndObjectAsync();
                }
            }
        }

        [RavenAction("/databases/*/indexes/debug", "GET", AuthorizationStatus.ValidUser)]
        public async Task Debug()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            var operation = GetStringQueryString("op");

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                if (string.Equals(operation, "map-reduce-tree", StringComparison.OrdinalIgnoreCase))
                {
                    if (index.Type.IsMapReduce() == false)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;

                        await context.WriteAsync(writer, new DynamicJsonValue
                        {
                            ["Error"] = $"{index.Name} is not map-reduce index"
                        });

                        return;
                    }

                    var docIds = GetStringValuesQueryString("docId", required: false);

                    using (index.GetReduceTree(docIds.ToArray(), out IEnumerable<ReduceTree> trees))
                    {
                        await writer.WriteReduceTrees(trees);
                    }

                    return;
                }

                if (string.Equals(operation, "source-doc-ids", StringComparison.OrdinalIgnoreCase))
                {
                    using (index.GetIdentifiersOfMappedDocuments(GetStringQueryString("startsWith", required: false), GetStart(), GetPageSize(), out IEnumerable<string> ids))
                    {
                        await writer.WriteArrayOfResultsAndCount(ids);
                    }

                    return;
                }

                if (string.Equals(operation, "entries-fields", StringComparison.OrdinalIgnoreCase))
                {
                    var fields = index.GetEntriesFields();

                    await writer.WriteStartObjectAsync();

                    await writer.WriteArrayAsync(nameof(fields.Static), fields.Static);
                    await writer.WriteCommaAsync();

                    await writer.WriteArrayAsync(nameof(fields.Dynamic), fields.Dynamic);

                    await writer.WriteEndObjectAsync();

                    return;
                }

                throw new NotSupportedException($"{operation} is not supported");
            }
        }

        [RavenAction("/databases/*/indexes", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task GetAll()
        {
            var name = GetStringQueryString("name", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();
            var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                IndexDefinition[] indexDefinitions;
                if (string.IsNullOrEmpty(name))
                    indexDefinitions = Database.IndexStore
                        .GetIndexes()
                        .OrderBy(x => x.Name)
                        .Skip(start)
                        .Take(pageSize)
                        .Select(x => x.GetIndexDefinition())
                        .ToArray();
                else
                {
                    var index = Database.IndexStore.GetIndex(name);
                    if (index == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    indexDefinitions = new[] { index.GetIndexDefinition() };
                }

                await writer.WriteStartObjectAsync();

                await writer.WriteArrayAsync(context, "Results", indexDefinitions, async (w, c, indexDefinition) =>
                {
                    if (namesOnly)
                    {
                        await w.WriteStringAsync(indexDefinition.Name);
                        return;
                    }

                    await w.WriteIndexDefinition(c, indexDefinition);
                });

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes/stats", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task Stats()
        {
            var name = GetStringQueryString("name", required: false);

            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
            {
                IndexStats[] indexStats;
                using (context.OpenReadTransaction())
                {
                    if (string.IsNullOrEmpty(name))
                    {
                        indexStats = Database.IndexStore
                            .GetIndexes()
                            .OrderBy(x => x.Name)
                            .Select(x =>
                            {
                                try
                                {
                                    return x.GetStats(calculateLag: true, calculateStaleness: true, calculateMemoryStats: true, queryContext: context);
                                }
                                catch (Exception e)
                                {
                                    if (Logger.IsOperationsEnabled)
                                        Logger.Operations($"Failed to get stats of '{x.Name}' index", e);

                                    try
                                    {
                                        Database.NotificationCenter.Add(AlertRaised.Create(Database.Name, $"Failed to get stats of '{x.Name}' index",
                                            $"Exception was thrown on getting stats of '{x.Name}' index",
                                            AlertType.Indexing_CouldNotGetStats, NotificationSeverity.Error, key: x.Name, details: new ExceptionDetails(e)));
                                    }
                                    catch (Exception addAlertException)
                                    {
                                        if (Logger.IsOperationsEnabled && addAlertException.IsOutOfMemory() == false && addAlertException.IsRavenDiskFullException() == false)
                                            Logger.Operations($"Failed to add alert when getting error on retrieving stats of '{x.Name}' index", addAlertException);
                                    }

                                    var state = x.State;

                                    if (e.IsOutOfMemory() == false && e.IsRavenDiskFullException() == false)
                                    {
                                        try
                                        {
                                            state = IndexState.Error;
                                            x.SetState(state, inMemoryOnly: true);
                                        }
                                        catch (Exception ex)
                                        {
                                            if (Logger.IsOperationsEnabled)
                                                Logger.Operations($"Failed to change state of '{x.Name}' index to error after encountering exception when getting its stats.",
                                                    ex);
                                        }
                                    }

                                    return new IndexStats
                                    {
                                        Name = x.Name,
                                        Type = x.Type,
                                        State = state,
                                        Status = x.Status,
                                        LockMode = x.Definition.LockMode,
                                        Priority = x.Definition.Priority,
                                    };
                                }
                            })
                            .ToArray();
                    }
                    else
                    {
                        var index = Database.IndexStore.GetIndex(name);
                        if (index == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return;
                        }

                        indexStats = new[] { index.GetStats(calculateLag: true, calculateStaleness: true, calculateMemoryStats: true, queryContext: context) };
                    }
                }

                await writer.WriteStartObjectAsync();

                await writer.WriteArrayAsync(context.Documents, "Results", indexStats, (w, c, stats) => w.WriteIndexStats(context.Documents, stats));

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes/staleness", "GET", AuthorizationStatus.ValidUser)]
        public async Task Stale()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            using (var context = QueryOperationContext.Allocate(Database, index))
            await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var stalenessReasons = new List<string>();
                var isStale = index.IsStale(context, stalenessReasons: stalenessReasons);

                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync("IsStale");
                await writer.WriteBoolAsync(isStale);
                await writer.WriteCommaAsync();

                await writer.WriteArrayAsync("StalenessReasons", stalenessReasons);

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes/progress", "GET", AuthorizationStatus.ValidUser)]
        public async Task Progress()
        {
            using (var context = QueryOperationContext.Allocate(Database, needsServerContext: true))
            await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Results");
                await writer.WriteStartArrayAsync();

                var first = true;
                foreach (var index in Database.IndexStore.GetIndexes())
                {
                    try
                    {
                        if (index.IsStale(context) == false)
                            continue;

                        var progress = index.GetProgress(context, isStale: true);

                        if (first == false)
                            await writer.WriteCommaAsync();

                        first = false;

                        await writer.WriteIndexProgress(context.Documents, progress);
                    }
                    catch (ObjectDisposedException)
                    {
                        // index was deleted
                    }
                    catch (OperationCanceledException)
                    {
                        // index was deleted
                    }
                    catch (Exception e)
                    {
                        if (Logger.IsOperationsEnabled)
                            Logger.Operations($"Failed to get index progress for index name: {index.Name}", e);
                    }
                }

                await writer.WriteEndArrayAsync();
                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes", "RESET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Reset()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            IndexDefinition indexDefinition;
            lock (Database)
            {
                var index = Database.IndexStore.ResetIndex(name);
                indexDefinition = index.GetIndexDefinition();
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WritePropertyNameAsync("Index");
                await writer.WriteIndexDefinition(context, indexDefinition);
                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/index/open-faulty-index", "POST", AuthorizationStatus.ValidUser)]
        public Task OpenFaultyIndex()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            if (index is FaultyInMemoryIndex == false)
                throw new InvalidOperationException($"Cannot open non faulty index named: {name}");

            lock (index)
            {
                var localIndex = Database.IndexStore.GetIndex(name);
                if (localIndex == null)
                    IndexDoesNotExistException.ThrowFor(name);

                if (localIndex is FaultyInMemoryIndex == false)
                    throw new InvalidOperationException($"Cannot open non faulty index named: {name}");

                Database.IndexStore.OpenFaultyIndex(localIndex);
            }

            return NoContent();
        }

        [RavenAction("/databases/*/indexes", "DELETE", AuthorizationStatus.ValidUser)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                var clientCert = GetCurrentCertificate();

                var auditLog = LoggingSource.AuditLog.GetLogger(Database.Name, "Audit");
                auditLog.Info($"Index {name} DELETE by {clientCert?.Subject} {clientCert?.Thumbprint}");
            }

            HttpContext.Response.StatusCode = await Database.IndexStore.TryDeleteIndexIfExists(name, GetRaftRequestIdFromQuery())
                ? (int)HttpStatusCode.NoContent
                : (int)HttpStatusCode.NotFound;
        }

        [RavenAction("/databases/*/indexes/c-sharp-index-definition", "GET", AuthorizationStatus.ValidUser)]
        public Task GenerateCSharpIndexDefinition()
        {
            var indexName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var index = Database.IndexStore.GetIndex(indexName);
            if (index == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            if (index.Type.IsAuto())
                throw new InvalidOperationException("Can't create C# index definition from auto indexes");

            var indexDefinition = index.GetIndexDefinition();

            using (var writer = new StreamWriter(ResponseBodyStream()))
            {
                var text = new IndexDefinitionCodeGenerator(indexDefinition).Generate();
                writer.Write(text);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/indexes/status", "GET", AuthorizationStatus.ValidUser)]
        public async Task Status()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();

                await writer.WritePropertyNameAsync(nameof(IndexingStatus.Status));
                await writer.WriteStringAsync(Database.IndexStore.Status.ToString());
                await writer.WriteCommaAsync();

                await writer.WritePropertyNameAsync(nameof(IndexingStatus.Indexes));
                await writer.WriteStartArrayAsync();
                var isFirst = true;
                foreach (var index in Database.IndexStore.GetIndexes())
                {
                    if (isFirst == false)
                        await writer.WriteCommaAsync();

                    isFirst = false;

                    await writer.WriteStartObjectAsync();

                    await writer.WritePropertyNameAsync(nameof(IndexingStatus.IndexStatus.Name));
                    await writer.WriteStringAsync(index.Name);

                    await writer.WriteCommaAsync();

                    await writer.WritePropertyNameAsync(nameof(IndexingStatus.IndexStatus.Status));
                    await writer.WriteStringAsync(index.Status.ToString());

                    await writer.WriteEndObjectAsync();
                }

                await writer.WriteEndArrayAsync();

                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes/set-lock", "POST", AuthorizationStatus.ValidUser)]
        public async Task SetLockMode()
        {
            var raftRequestId = GetRaftRequestIdFromQuery();
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/set-lock");
                var parameters = JsonDeserializationServer.Parameters.SetIndexLockParameters(json);

                if (parameters.IndexNames == null || parameters.IndexNames.Length == 0)
                    throw new ArgumentNullException(nameof(parameters.IndexNames));

                // Check for auto-indexes - we do not set lock for auto-indexes
                if (parameters.IndexNames.Any(indexName => indexName.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase)))
                {
                    throw new InvalidOperationException("'Indexes list contains Auto-Indexes. Lock Mode' is not set for Auto-Indexes.");
                }

                for (var index = 0; index < parameters.IndexNames.Length; index++)
                {
                    var name = parameters.IndexNames[index];
                    await Database.IndexStore.SetLock(name, parameters.Mode, $"{raftRequestId}/{index}");
                }
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/indexes/set-priority", "POST", AuthorizationStatus.ValidUser)]
        public async Task SetPriority()
        {
            var raftRequestId = GetRaftRequestIdFromQuery();
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/set-priority");
                var parameters = JsonDeserializationServer.Parameters.SetIndexPriorityParameters(json);

                for (var index = 0; index < parameters.IndexNames.Length; index++)
                {
                    var name = parameters.IndexNames[index];
                    await Database.IndexStore.SetPriority(name, parameters.Priority, $"{raftRequestId}/{index}");
                }

                NoContentStatus();
            }
        }

        [RavenAction("/databases/*/indexes/errors", "DELETE", AuthorizationStatus.ValidUser)]
        public Task ClearErrors()
        {
            var names = GetStringValuesQueryString("name", required: false);

            var indexes = new List<Index>();

            if (names.Count == 0)
                indexes.AddRange(Database.IndexStore.GetIndexes());
            else
            {
                foreach (var name in names)
                {
                    var index = Database.IndexStore.GetIndex(name);
                    if (index == null)
                        IndexDoesNotExistException.ThrowFor(name);

                    indexes.Add(index);
                }
            }

            foreach (var index in indexes)
                index.DeleteErrors();

            return NoContent();
        }

        [RavenAction("/databases/*/indexes/errors", "GET", AuthorizationStatus.ValidUser, IsDebugInformationEndpoint = true)]
        public async Task GetErrors()
        {
            var names = GetStringValuesQueryString("name", required: false);

            List<Index> indexes;
            if (names.Count == 0)
                indexes = Database.IndexStore.GetIndexes().ToList();
            else
            {
                indexes = new List<Index>();
                foreach (var name in names)
                {
                    var index = Database.IndexStore.GetIndex(name);
                    if (index == null)
                        IndexDoesNotExistException.ThrowFor(name);

                    indexes.Add(index);
                }
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WriteStartObjectAsync();
                await writer.WriteArrayAsync(context, "Results", indexes, async (w, c, index) =>
                {
                    await w.WriteStartObjectAsync();
                    await w.WritePropertyNameAsync("Name");
                    await w.WriteStringAsync(index.Name);
                    await w.WriteCommaAsync();
                    await BlittableJsonTextWriterExtensions.WriteArrayAsync(w, c, "Errors", index.GetErrors(), async (ew, ec, error) =>
                    {
                        await ew.WriteStartObjectAsync();
                        await ew.WritePropertyNameAsync(nameof(error.Timestamp));
                        await ew.WriteDateTimeAsync(error.Timestamp, isUtc: true);
                        await ew.WriteCommaAsync();

                        await ew.WritePropertyNameAsync(nameof(error.Document));
                        await ew.WriteStringAsync(error.Document);
                        await ew.WriteCommaAsync();

                        await ew.WritePropertyNameAsync(nameof(error.Action));
                        await ew.WriteStringAsync(error.Action);
                        await ew.WriteCommaAsync();

                        await ew.WritePropertyNameAsync(nameof(error.Error));
                        await ew.WriteStringAsync(error.Error);
                        await ew.WriteEndObjectAsync();
                    });
                    await w.WriteEndObjectAsync();
                });
                await writer.WriteEndObjectAsync();
            }
        }

        [RavenAction("/databases/*/indexes/terms", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Terms()
        {
            var field = GetQueryStringValueAndAssertIfSingleAndNotEmpty("field");

            using (var token = CreateTimeLimitedOperationToken())
            using (var context = QueryOperationContext.Allocate(Database))
            {
                var name = GetIndexNameFromCollectionAndField(field) ?? GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

                var fromValue = GetStringQueryString("fromValue", required: false);
                var existingResultEtag = GetLongFromHeaders("If-None-Match");

                var result = Database.QueryRunner.ExecuteGetTermsQuery(name, field, fromValue, existingResultEtag, GetPageSize(), context, token, out var index);

                if (result.NotModified)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

                await using (var writer = new AsyncBlittableJsonTextWriter(context.Documents, ResponseBodyStream()))
                {
                    if (field.EndsWith("__minX") ||
                        field.EndsWith("__minY") ||
                        field.EndsWith("__maxX") ||
                        field.EndsWith("__maxY"))
                    {
                        if (index.Definition.IndexFields != null &&
                            index.Definition.IndexFields.TryGetValue(field.Substring(0, field.Length - 6), out var indexField) == true)
                        {
                            if (indexField.Spatial?.Strategy == Client.Documents.Indexes.Spatial.SpatialSearchStrategy.BoundingBox)
                            {
                                // Term-values for 'Spatial Index Fields' with 'BoundingBox' are encoded in Lucene as 'prefixCoded bytes'
                                // Need to convert to numbers for the Studio
                                var readableTerms = new HashSet<string>();
                                foreach (var item in result.Terms)
                                {
                                    var num = Lucene.Net.Util.NumericUtils.PrefixCodedToDouble(item);
                                    readableTerms.Add(NumberUtil.NumberToString(num));
                                }

                                result.Terms = readableTerms;
                            }
                        }
                    }

                    await writer.WriteTermsQueryResult(context.Documents, result);
                }
            }
        }

        private string GetIndexNameFromCollectionAndField(string field)
        {
            var collection = GetStringQueryString("collection", false);
            if (string.IsNullOrEmpty(collection))
                return null;
            var query = new IndexQueryServerSide(new QueryMetadata($"from {collection} select {field}", null, 0));
            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(Database.IndexStore);
            var match = dynamicQueryToIndex.Match(DynamicQueryMapping.Create(query));
            if (match.MatchType == DynamicQueryMatchType.Complete ||
                match.MatchType == DynamicQueryMatchType.CompleteButIdle)
                return match.IndexName;
            throw new IndexDoesNotExistException($"There is no index to answer the following query: from {collection} select {field}");
        }

        [RavenAction("/databases/*/indexes/total-time", "GET", AuthorizationStatus.ValidUser)]
        public async Task TotalTime()
        {
            var indexes = GetIndexesToReportOn();
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var dja = new DynamicJsonArray();

                foreach (var index in indexes)
                {
                    DateTime baseLine = DateTime.MinValue;
                    using (context.OpenReadTransaction())
                    {
                        foreach (var collection in index.Collections)
                        {
                            switch (index.SourceType)
                            {
                                case IndexSourceType.Documents:
                                    var etag = Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
                                    var document = Database.DocumentsStorage.GetDocumentsFrom(context, collection, etag, 0, 1, DocumentFields.Default).FirstOrDefault();
                                    if (document != null && document.LastModified > baseLine)
                                        baseLine = document.LastModified;
                                    break;

                                case IndexSourceType.TimeSeries:
                                    break;

                                default:
                                    throw new NotSupportedException($"Index with source type '{index.SourceType}' is not supported.");
                            }
                        }
                    }
                    var createdTimestamp = index.GetStats().CreatedTimestamp;
                    if (createdTimestamp > baseLine)
                        baseLine = createdTimestamp;

                    var lastBatch = index.GetIndexingPerformance()
                                    .LastOrDefault(x => x.Completed != null)
                                    ?.Completed ?? DateTime.UtcNow;

                    dja.Add(new DynamicJsonValue
                    {
                        ["Name"] = index.Name,
                        ["TotalIndexingTime"] = index.TimeSpentIndexing.Elapsed.ToString("c"),
                        ["LagTime"] = (lastBatch - baseLine).ToString("c")
                    });
                }

                await context.WriteAsync(writer, dja);
            }
        }

        [RavenAction("/databases/*/indexes/performance", "GET", AuthorizationStatus.ValidUser)]
        public async Task Performance()
        {
            var stats = GetIndexesToReportOn()
                .Select(x => new IndexPerformanceStats
                {
                    Name = x.Name,
                    Performance = x.GetIndexingPerformance()
                })
                .ToArray();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await writer.WritePerformanceStatsAsync(context, stats);
            }
        }

        [RavenAction("/databases/*/indexes/performance/live", "GET", AuthorizationStatus.ValidUser, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var indexNames = GetIndexesToReportOn().Select(x => x.Name).ToList();
                if (GetBoolValueQueryString("includeSideBySide", false) ?? false)
                {
                    // user requested to track side by side indexes as well
                    // add extra names to indexNames list
                    var complementaryIndexes = new HashSet<string>();
                    foreach (var indexName in indexNames)
                    {
                        if (indexName.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                            complementaryIndexes.Add(indexName.Substring(Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length));
                        else
                            complementaryIndexes.Add(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexName);
                    }

                    indexNames.AddRange(complementaryIndexes);
                }

                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                using (var ms = new MemoryStream())
                using (var collector = new LiveIndexingPerformanceCollector(Database, indexNames))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        [RavenAction("/databases/*/indexes/suggest-index-merge", "GET", AuthorizationStatus.ValidUser)]
        public async Task SuggestIndexMerge()
        {
            var mergeIndexSuggestions = Database.IndexStore.ProposeIndexMergeSuggestions();

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                await context.WriteAsync(writer, mergeIndexSuggestions.ToJson());
                await writer.FlushAsync();
            }
        }

        [RavenAction("/databases/*/indexes/try", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task TestJavaScriptIndex()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "TestJavaScriptIndex");
                if (input.TryGet("Definition", out BlittableJsonReaderObject index) == false)
                    ThrowRequiredPropertyNameInRequest("Definition");

                input.TryGet("Ids", out BlittableJsonReaderArray ids);

                var indexDefinition = JsonDeserializationServer.IndexDefinition(index);

                if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                    throw new ArgumentException("Index must have a 'Maps' fields");

                indexDefinition.Type = indexDefinition.DetectStaticIndexType();

                if (indexDefinition.Type.IsJavaScript() == false)
                    throw new UnauthorizedAccessException("Testing indexes is only allowed for JavaScript indexes.");

                var compiledIndex = new JavaScriptIndex(indexDefinition, Database.Configuration);

                var inputSize = GetIntValueQueryString("inputSize", false) ?? defaultInputSizeForTestingJavaScriptIndex;
                var collections = new HashSet<string>(compiledIndex.Maps.Keys);
                var docsPerCollection = new Dictionary<string, List<DynamicBlittableJson>>();
                using (context.OpenReadTransaction())
                {
                    if (ids == null)
                    {
                        foreach (var collection in collections)
                        {
                            docsPerCollection.Add(collection,
                                Database.DocumentsStorage.GetDocumentsFrom(context, collection, 0, 0, inputSize).Select(d => new DynamicBlittableJson(d)).ToList());
                        }
                    }
                    else
                    {
                        var listOfIds = ids.Select(x => x.ToString());
                        var _ = new Reference<int>
                        {
                            Value = 0
                        };
                        var docs = Database.DocumentsStorage.GetDocuments(context, listOfIds, 0, long.MaxValue, _);
                        foreach (var doc in docs)
                        {
                            if (doc.TryGetMetadata(out var metadata) && metadata.TryGet(Constants.Documents.Metadata.Collection, out string collectionStr))
                            {
                                if (docsPerCollection.TryGetValue(collectionStr, out var listOfDocs) == false)
                                {
                                    listOfDocs = docsPerCollection[collectionStr] = new List<DynamicBlittableJson>();
                                }
                                listOfDocs.Add(new DynamicBlittableJson(doc));
                            }
                        }
                    }

                    var mapRes = new List<ObjectInstance>();
                    //all maps
                    foreach (var listOfFunctions in compiledIndex.Maps)
                    {
                        //multi maps per collection
                        foreach (var kvp in listOfFunctions.Value)
                        {
                            // TODO [ppekrol] check if this is correct
                            foreach (var mapFunc in kvp.Value)
                            {
                                if (docsPerCollection.TryGetValue(listOfFunctions.Key, out var docs))
                                {
                                    foreach (var res in mapFunc(docs))
                                    {
                                        mapRes.Add((ObjectInstance)res);
                                    }
                                }
                            }
                        }
                    }
                    var first = true;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        await writer.WriteStartObjectAsync();
                        await writer.WritePropertyNameAsync("MapResults");
                        await writer.WriteStartArrayAsync();
                        foreach (var mapResult in mapRes)
                        {
                            if (JavaScriptIndexUtils.StringifyObject(mapResult) is JsString jsStr)
                            {
                                if (first == false)
                                {
                                    await writer.WriteCommaAsync();
                                }
                                await writer.WriteStringAsync(jsStr.ToString());
                                first = false;
                            }
                        }
                        await writer.WriteEndArrayAsync();
                        if (indexDefinition.Reduce != null)
                        {
                            using (var bufferPool = new UnmanagedBuffersPoolWithLowMemoryHandling("JavaScriptIndexTest", Database.Name))
                            {
                                compiledIndex.SetBufferPoolForTestingPurposes(bufferPool);
                                compiledIndex.SetAllocatorForTestingPurposes(context.Allocator);
                                first = true;
                                await writer.WritePropertyNameAsync("ReduceResults");
                                await writer.WriteStartArrayAsync();

                                var reduceResults = compiledIndex.Reduce(mapRes.Select(mr => new DynamicBlittableJson(JsBlittableBridge.Translate(context, mr.Engine, mr))));

                                foreach (JsValue reduceResult in reduceResults)
                                {
                                    if (JavaScriptIndexUtils.StringifyObject(reduceResult) is JsString jsStr)
                                    {
                                        if (first == false)
                                        {
                                            await writer.WriteCommaAsync();
                                        }

                                        await writer.WriteStringAsync(jsStr.ToString());
                                        first = false;
                                    }
                                }
                            }

                            await writer.WriteEndArrayAsync();
                        }
                        await writer.WriteEndObjectAsync();
                    }
                }
            }
        }

        private static readonly int defaultInputSizeForTestingJavaScriptIndex = 10;

        private IEnumerable<Index> GetIndexesToReportOn()
        {
            IEnumerable<Index> indexes;
            var names = HttpContext.Request.Query["name"];

            if (names.Count == 0)
                indexes = Database.IndexStore
                    .GetIndexes();
            else
            {
                indexes = Database.IndexStore
                    .GetIndexes()
                    .Where(x => names.Contains(x.Name, StringComparer.OrdinalIgnoreCase));
            }
            return indexes;
        }
    }
}
