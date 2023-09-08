using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Corax;
using Corax.Analyzers;
using Corax.Pipeline;
using Corax.Utils;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal struct CoraxDocumentTrainEnumerator : IReadOnlySpanEnumerator
{
    private sealed class Builder : IndexWriter.IIndexEntryBuilder
    {
        private readonly ByteStringContext _allocator;
        private readonly List<(int FieldId, string FieldName, ByteString Value)> _terms;

        public Builder(ByteStringContext allocator,List<(int FieldId, string FieldName, ByteString)> terms)
        {
            _allocator = allocator;
            _terms = terms;
        }

        public void Boost(float boost)
        {
            
        }

        public ReadOnlySpan<byte> AnalyzeSingleTerm(int fieldId, ReadOnlySpan<byte> value)
        {
            return value; // not applicable 
        }

        public void WriteNull(int fieldId, string path)
        {
            _allocator.From(global::Corax.Constants.NullValueSlice.AsSpan(), out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, null, b));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, string path, string value)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, null, b));
        }

        public void Write(int fieldId, string path, string value, long longValue, double dblValue)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
        {
            if (value.Length == 0)
                return;
            _allocator.From(value, out var b);
            _terms.Add((fieldId, path, b));
        }

        public void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry)
        {
            // nothing to do here
        }

        public void Store(BlittableJsonReaderObject storedValue)
        {
            // nothing to do
        }

        public void RegisterEmptyOrNull(int fieldId, string fieldName, StoredFieldType type)
        {
            // nothing to do
        }

        public void Store(int fieldId, string name, BlittableJsonReaderObject storedValue)
        {
            // nothing to do
        }

        public void IncrementList()
        {
            
        }

        public int ResetList()
        {
            return default;
        }

        public void RestoreList(int old)
        {
        }

        public void DecrementList()
        {
        }
    }

    private readonly DocumentsStorage _documentStorage;
    private readonly DocumentsOperationContext _docsContext;
    private readonly TransactionOperationContext _indexContext;
    private readonly Index _index;
    private readonly IndexType _indexType;
    private readonly CoraxDocumentConverterBase _converter;
    private readonly HashSet<string> _collections;
    private readonly int _take;
    private IEnumerator<ArraySegment<byte>> _itemsEnumerable;
    private readonly List<(int FieldId, string FieldName, ByteString Value)> _terms;
    private readonly Builder _builder;
    private readonly CancellationToken _token;
    private readonly Size _maxAllocatedMemory;

    public CoraxDocumentTrainEnumerator(TransactionOperationContext indexContext, CoraxDocumentConverterBase converter, Index index, IndexType indexType, DocumentsStorage storage, DocumentsOperationContext docsContext, HashSet<string> collections, CancellationToken token, int take = int.MaxValue)
    {
        _indexContext = indexContext;
        _index = index;
        _indexType = indexType;
        _converter = converter;
        _take = take;
        _token = token;

        // RavenDB-21043: Tracking the total memory allocated by the thread is also a way to limit the total resources allocated
        // to the training process. We are currently limiting the default to 2Gb and we haven't seen any deterioration in the 
        // compression using that limit. However, given there is no limitation in 64bits mode, we could increase it if we find
        // cases which are not covered.
        _maxAllocatedMemory = _index.Configuration.MaxAllocationsAtDictionaryTraining;

        _documentStorage = storage;
        _docsContext = docsContext;
        _collections = collections;
        _terms = new List<(int FieldId, string FieldName, ByteString Value)>();
        _builder = new Builder(indexContext.Allocator, _terms);
    }

    private IEnumerable<ArraySegment<byte>> GetItems()
    {
        var lowercaseAnalyzer = Analyzer.CreateLowercaseAnalyzer(_indexContext.Allocator);
        var scope = new IndexingStatsScope(new IndexingRunStats());
        
        var wordsBuffer = new byte[1024];
        var tokenBuffer = new Token[1024];

        // RavenDB-21043: Track the total allocations that we will allow each collection to use. The idea is that multi-collection indexes
        // use this number to also ensure that all collections have the opportunity to give samples to the training process.
        var maxAllocatedMemoryPerCollection = _maxAllocatedMemory / _collections.Count;

        foreach (var collection in _collections)
        {
            // We retrieve the baseline memory in order to calculate the difference.
            var atStartAllocated = new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes);

            if (atStartAllocated > new Size(1, SizeUnit.Gigabytes))
                //if (atStartAllocated > new Size(2, SizeUnit.Gigabytes))
            {
                Console.WriteLine("AAAAAAAAAAAA");

                Debugger.Launch();
                Debugger.Break();
            }

            using var itemEnumerator = _index.GetMapEnumerator(GetItemsEnumerator(_docsContext, collection, _take, _token), collection, _indexContext, scope, _indexType);
            while (true)
            {
                if (itemEnumerator.MoveNext(_docsContext, out var mapResults, out _) == false)
                    break;

                var doc = (Document)itemEnumerator.Current.Item;

                var fields = _converter.GetKnownFieldsForWriter();

                foreach (var result in mapResults)
                {
                    _terms.Clear();
                    _converter.SetDocument(doc.LowerId, null, result, _indexContext,_builder);
                    
                    for (int i = 0; i < _terms.Count; i++)
                    {
                        var (fieldId, fieldName, value) = _terms[i];

                        if (fields.TryGetByFieldId(fieldId, out var field) == false &&
                            fields.TryGetByFieldName(fieldName, out field) == false)
                            continue;
                        
                        var analyzer = field.Analyzer ?? lowercaseAnalyzer;

                        
                        if (value.Length < 3)
                            continue;
                    
                        if (value.Length > wordsBuffer.Length)
                        {
                            wordsBuffer = new byte[value.Length * 2];
                            tokenBuffer = new Token[value.Length * 2];
                        }
                    
                        int items;
                        {
                            var wordsSpan = wordsBuffer.AsSpan();
                            var tokenSpan = tokenBuffer.AsSpan();
                            analyzer.Execute(value.ToSpan(), ref wordsSpan, ref tokenSpan);
                            items = tokenSpan.Length;
                        }

                        // We want to have a good sample but at the same time not overburden the training process.
                        // Therefore, we will start advancing faster the more tokens there are. This is specially
                        // relevant in cases where we have to deal with full text search of big documents.
                        int advance = items / 16 + 1;
                        for (int j = 0; j < items; j += advance)
                        {
                            int length = (int)tokenBuffer[j].Length;
                            int offset = tokenBuffer[j].Offset;
                            if (length > 128)
                            {
                                // Very unlikely case of indexes without analyzers that are extremely large.
                                offset += Random.Shared.Next(length - 128);
                                length = 128;
                            }
                            yield return new ArraySegment<byte>(wordsBuffer, offset, length);
                        }
                    }
                }

                // Check if we have already hit the threshold allocations.
                var totalAllocated = new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes) - atStartAllocated;
                if (totalAllocated > maxAllocatedMemoryPerCollection)
                {
                    Console.WriteLine("Break");
                    break;
                }

                Console.WriteLine($"Total allocated: {new Size(NativeMemory.CurrentThreadStats.TotalAllocated, SizeUnit.Bytes)}, allocated diff: {totalAllocated}, at start - {atStartAllocated}");
            }
        }
    }

    private IEnumerator<Document> GetDocumentsEnumerator(DocumentsOperationContext docsContext, string collection, long take, CancellationToken token)
    {
        var size = docsContext.DocumentDatabase.Configuration.Databases.PulseReadTransactionLimit;
        var coraxDocumentTrainDocumentSource = new CoraxDocumentTrainSourceEnumerator(_documentStorage);
        
        if (collection == Constants.Documents.Collections.AllDocumentsCollection)
            return new TransactionForgetAboutDocumentEnumerator(new PulsedTransactionEnumerator<Document, CoraxDocumentTrainSourceState>(docsContext,
                state => coraxDocumentTrainDocumentSource.GetDocumentsForDictionaryTraining(docsContext, state), new(docsContext, size, take, token)), docsContext); 

        return new TransactionForgetAboutDocumentEnumerator(new PulsedTransactionEnumerator<Document,CoraxDocumentTrainSourceState>(docsContext, 
            state =>  coraxDocumentTrainDocumentSource.GetDocumentsForDictionaryTraining(docsContext, collection, state)
            , new CoraxDocumentTrainSourceState(docsContext, size, take, token)), docsContext);
    }

    private IEnumerable<IndexItem> GetItemsEnumerator(DocumentsOperationContext docsContext, string collection, long take, CancellationToken token)
    {
        foreach (var document in GetDocumentsEnumerator(docsContext, collection, take, token))
        {
            yield return new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document);
        }
    }

    public void Reset()
    {
        _itemsEnumerable = GetItems().GetEnumerator();
    }

    public bool MoveNext(out ReadOnlySpan<byte> output)
    {
        _itemsEnumerable ??= GetItems().GetEnumerator();

        // RavenDB-21106: Since the training of dictionaries may cause us to trigger (critical) errors prematurely as without training
        // they would trigger during indexing and we don't want to replicate all the handling necessary for it. We will just ignore any
        // document where an error may happen during indexing, since it will also happen there and handled appropriately. 
        bool result;
        while (true)
        {
            try
            {
                result = _itemsEnumerable.MoveNext();
                break;
            }
            catch 
            {
                // Since there was an error, we will ignore this document and try again.
            }
        }

        if (result == false)
        {
            output = ReadOnlySpan<byte>.Empty;
            return false;
        }

        var current = _itemsEnumerable.Current;
        output = current.AsSpan();
        return true;
    }
}
