﻿using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Voron.Debugging;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class MapReduceResultsStore : IDisposable
    {
        public const string ReduceTreePrefix = "__raven/map-reduce/#reduce-tree-";
        public const string NestedValuesPrefix = "__raven/map-reduce/#nested-section-";

        private readonly ulong _reduceKeyHash;
        private readonly TransactionOperationContext _indexContext;
        private readonly MapReduceIndexingContext _mapReduceContext;
        private readonly Slice _nestedValueKey;
        private ByteStringContext.InternalScope _nestedValueKeyScope;
        private readonly Transaction _tx;

        private NestedMapResultsSection _nestedSection;

        public MapResultsStorageType Type { get; private set; }

        public Tree Tree;
        public HashSet<long> ModifiedPages;
        public HashSet<long> FreedPages;

        public MapReduceResultsStore(ulong reduceKeyHash, MapResultsStorageType type, TransactionOperationContext indexContext, MapReduceIndexingContext mapReduceContext, bool create)
        {
            _reduceKeyHash = reduceKeyHash;
            Type = type;
            _indexContext = indexContext;
            _mapReduceContext = mapReduceContext;
            _tx = indexContext.Transaction.InnerTransaction;

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    InitializeTree(create);
                    break;
                case MapResultsStorageType.Nested:
                    _nestedValueKeyScope = Slice.From(indexContext.Allocator, NestedValuesPrefix + reduceKeyHash, ByteStringType.Immutable, out _nestedValueKey);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void InitializeTree(bool create)
        {
            var treeName = ReduceTreePrefix + _reduceKeyHash;
            var options = _tx.LowLevelTransaction.Environment.Options.RunningOn32Bits ? TreeFlags.None : TreeFlags.LeafsCompressed;
            Tree = create ? _tx.CreateTree(treeName, flags: options) : _tx.ReadTree(treeName);

            //if (_reduceKeyHash == 15467709870019659167)
            //{
            //   DebugStuff.RenderAndShow(Tree);

            //    Tree.ValidateTree_References();


            //    TreePage parent = Tree.GetReadOnlyTreePage(136148);

            //    TreePage page = Tree.GetReadOnlyTreePage(136140);



            //    long parentPageOf = Tree.GetParentPageOf(Tree.DecompressPage(page, skipCache: true));


            //    //page.GetNodeKey(_tx.LowLevelTransaction, 0, out var key);

            //    //Slice separator = Slices.Empty;
            //    //for (int i = 0; i < parent.NumberOfEntries; i++)
            //    //{
            //    //    if (parent.GetNode(i)->PageNumber == page.PageNumber)
            //    //    {
            //    //        parent.GetNodeKey(_tx.LowLevelTransaction, i, out separator);

            //    //        int compare = SliceComparer.Compare(key, separator);
            //    //    }
            //    //}

            //    //int hit = 0;

            //    //for (int i = 0; i < page.NumberOfEntries; i++)
            //    //{
            //    //    using (page.GetNodeKey(_tx.LowLevelTransaction, i, out var key2))
            //    //    {
            //    //        if (SliceComparer.Compare(key2, separator) < 0)
            //    //        {
            //    //            System.Console.WriteLine($"DDDDDDDD: {key2}, {separator}, {i}");

            //    //            hit++;

            //    //            //Debugger.Launch();
            //    //            //Debugger.Break();
            //    //        }
            //    //    }
            //    //}

            //    //var decompressed = Tree.DecompressPage(page, skipCache: true);

            //    //Tree.RemoveEmptyDecompressedPage(decompressed);
            //}

            ModifiedPages = new HashSet<long>();
            FreedPages = new HashSet<long>();

            _mapReduceContext.PageModifiedInReduceTree += page => FreedPages.Remove(page);

            Tree.PageModified += (page, flags) =>
            {
                if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                    return;

                ModifiedPages.Add(page);
                _mapReduceContext.OnPageModifiedInReduceTree(page);
            };

            Tree.PageFreed += (page, flags) =>
            {
                if ((flags & PageFlags.Overflow) == PageFlags.Overflow)
                    return;

                FreedPages.Add(page);
                ModifiedPages.Remove(page);
            };
        }

        public void Delete(long id)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                        Tree.Delete(entrySlice);
                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    section.Delete(id);

                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        public void Add(long id, BlittableJsonReaderObject result)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    
                    using (Slice.External(_indexContext.Allocator, (byte*) &id, sizeof(long), out entrySlice))
                    {
                        using (Tree.DirectAdd(entrySlice, result.Size, out byte* ptr))
                            result.CopyTo(ptr);
                    }

                    break;
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();

                    if (_mapReduceContext.ReducePhaseTree.ShouldGoToOverflowPage(_nestedSection.SizeAfterAdding(result)))
                    {
                        // would result in an overflow, that would be a space waste anyway, let's move to tree mode
                        MoveExistingResultsToTree(section);
                        Add(Bits.SwapBytes(id), result); // now re-add the value, revert id to its original value
                    }
                    else
                    {
                        section.Add(id, result);
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        public ReadMapEntryScope Get(long id)
        {
            id = Bits.SwapBytes(id);

            switch (Type)
            {
                case MapResultsStorageType.Tree:
                    Slice entrySlice;
                    using (Slice.External(_indexContext.Allocator, (byte*)&id, sizeof(long), out entrySlice))
                    {
                        if (Tree.IsLeafCompressionSupported)
                        {
                            var read = Tree.ReadDecompressed(entrySlice);

                            if (read == null)
                                throw new InvalidOperationException($"Could not find a map result with id '{id}' in '{Tree.Name}' tree");

                            return new ReadMapEntryScope(read);
                        }
                        else
                        {
                            var read = Tree.Read(entrySlice);

                            if (read == null)
                                throw new InvalidOperationException($"Could not find a map result with id '{id}' in '{Tree.Name}' tree");

                            return new ReadMapEntryScope(PtrSize.Create(read.Reader.Base, read.Reader.Length));
                        }
                    }
                case MapResultsStorageType.Nested:
                    var section = GetNestedResultsSection();
                    return new ReadMapEntryScope(section.Get(id));
                default:
                    throw new ArgumentOutOfRangeException(Type.ToString());
            }
        }

        private void MoveExistingResultsToTree(NestedMapResultsSection section)
        {
            Type = MapResultsStorageType.Tree;

            var byteType = (byte)Type;

            using (Slice.External(_indexContext.Allocator, &byteType, sizeof(byte), out Slice val))
                _mapReduceContext.ResultsStoreTypes.Add((long)_reduceKeyHash, val);
            InitializeTree(create: true);

            section.MoveTo(Tree);
        }

        public NestedMapResultsSection GetNestedResultsSection(Tree tree = null)
        {
            if (_nestedSection != null)
                return _nestedSection;

            _nestedSection = new NestedMapResultsSection(_indexContext.Environment, tree ?? _mapReduceContext.ReducePhaseTree, _nestedValueKey);

            return _nestedSection;
        }

        public void Dispose()
        {
            _nestedValueKeyScope.Dispose();
            Tree?.Dispose();
        }
    }
}
