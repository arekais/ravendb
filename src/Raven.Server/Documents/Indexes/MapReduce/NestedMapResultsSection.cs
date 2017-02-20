﻿using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Paging;
using System;
using Voron.Util;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class NestedMapResultsSection
    {
        private readonly StorageEnvironment _env;
        private readonly Tree _parent;
        private readonly Slice _nestedValueKey;

        private int _dataSize;

        public NestedMapResultsSection(StorageEnvironment env,Tree parent, Slice nestedValueKey)
        {
            _env = env;
            _parent = parent;
            _nestedValueKey = nestedValueKey;
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult != null)
            {
                _dataSize = readResult.Reader.Length;
                IsNew = false;
            }
            else
            {
                IsNew = true;
            }
        }

        public bool IsNew { get; }

        public int Size => _dataSize;

        public bool IsModified { get; private set; }

        public Slice Name => _nestedValueKey;

        public TreePage RelevantPage
        {
            get
            {
                TreeNodeHeader* node;
                return _parent.FindPageFor(_nestedValueKey, out node);
            }
        }

        public void Add(long id, BlittableJsonReaderObject result)
        {
            IsModified = true;

            TemporaryPage tmp;
            using (_env.GetTemporaryPage(_parent.Llt, out tmp))
            {
                var dataPosInTempPage = 0;
                var readResult = _parent.Read(_nestedValueKey);
                if (readResult != null)
                {
                    var reader = readResult.Reader;
                    var entry = (ResultHeader*)reader.Base;
                    var end = reader.Base + reader.Length;
                    while (entry < end)
                    {
                        if (entry->Id == id)
                        {
                            if (entry->Size != result.Size)
                            {
                                Delete(id);
                                readResult = _parent.Read(_nestedValueKey);
                                break;
                            }
                            using (var dataStart = _parent.DirectAdd(_nestedValueKey, reader.Length))
                            {
                                var pos = dataStart.Ptr + ((byte*)entry - reader.Base + sizeof(ResultHeader));
                                Memory.Copy(pos, result.BasePointer, result.Size);
                                return;// just overwrite it completely
                            }
                        }
                        entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
                    }
                    
                    if (readResult != null)
                    {
                        Memory.Copy(tmp.TempPagePointer, readResult.Reader.Base, readResult.Reader.Length);
                        dataPosInTempPage = readResult.Reader.Length;
                    }
                }

                Debug.Assert(dataPosInTempPage + sizeof(ResultHeader) + result.Size <= tmp.TempPageBuffer.Length);
                var newEntry = (ResultHeader*)(tmp.TempPagePointer + dataPosInTempPage);
                newEntry->Id = id;
                newEntry->Size = (ushort)result.Size;
                Memory.Copy(tmp.TempPagePointer + dataPosInTempPage + sizeof(ResultHeader), result.BasePointer, result.Size);
                dataPosInTempPage += result.Size + sizeof(ResultHeader);
                using (var dest = _parent.DirectAdd(_nestedValueKey, dataPosInTempPage))
                    Memory.Copy(dest.Ptr, tmp.TempPagePointer, dataPosInTempPage);

                _dataSize += result.Size + sizeof(ResultHeader);
            }

        }
        public PtrSize Get(long id)
        {
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult == null)
                throw new InvalidOperationException($"Could not find a map result wit id '{id}' within a nested values section stored under '{_nestedValueKey}' key");

            var reader = readResult.Reader;
            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            while (entry < end)
            {
                if (entry->Id == id)
                {
                    return PtrSize.Create((byte*)entry + sizeof(ResultHeader), entry->Size);
                }

                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }

            throw new InvalidOperationException($"Could not find a map result wit id '{id}' within a nested values section stored under '{_nestedValueKey}' key");
        }

        public void MoveTo(Tree newHome)
        {
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult == null)
                return;

            var reader = readResult.Reader;
            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            long currentId;
            Slice key;
            using(Slice.External(_parent.Llt.Allocator,(byte*)&currentId, sizeof(long),out key))
            while (entry < end)
            {
                currentId = entry->Id;

                using (var item = newHome.DirectAdd(key, entry->Size))
                    Memory.Copy(item.Ptr, (byte*)entry + sizeof(ResultHeader), entry->Size);

                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }
            _parent.Delete(_nestedValueKey);
        }

        public int GetResults(JsonOperationContext context, List<BlittableJsonReaderObject> results)
        {
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult == null)
                return 0;

            var reader = readResult.Reader;
            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            int entries = 0;
            while (entry < end)
            {
                entries++;
                results.Add(new BlittableJsonReaderObject((byte*) entry + sizeof(ResultHeader), entry->Size, context));
                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }
            return entries;
        }

        public int GetResultsForDebug(JsonOperationContext context, Dictionary<long, BlittableJsonReaderObject> results)
        {
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult == null)
                return 0;

            var reader = readResult.Reader;
            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            int entries = 0;
            while (entry < end)
            {
                entries++;
                results.Add(entry->Id, new BlittableJsonReaderObject((byte*)entry + sizeof(ResultHeader), entry->Size, context));
                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }
            return entries;
        }

        public void Delete(long id)
        {
            IsModified = true;

            TemporaryPage tmp;
            using (_env.GetTemporaryPage(_parent.Llt, out tmp))
            {
                var readResult = _parent.Read(_nestedValueKey);
                if (readResult == null)
                    return;
                var reader = readResult.Reader;
                var entry = (ResultHeader*) reader.Base;
                var end = reader.Base + reader.Length;
                while (entry < end)
                {
                    if (entry->Id != id)
                    {
                        entry = (ResultHeader*) ((byte*) entry + sizeof(ResultHeader) + entry->Size);
                        continue;
                    }

                    var copiedDataStart = (byte*) entry - reader.Base;
                    var copiedDataEnd = end - ((byte*) entry + sizeof(ResultHeader) + entry->Size);
                    if (copiedDataEnd == 0 && copiedDataStart == 0)
                    {
                        _parent.Delete(_nestedValueKey);
                        _dataSize = 0;
                        break;
                    }
                    Memory.Copy(tmp.TempPagePointer, reader.Base, copiedDataStart);
                    Memory.Copy(tmp.TempPagePointer + copiedDataStart,
                        reader.Base + copiedDataStart + sizeof(ResultHeader) + entry->Size, copiedDataEnd);

                    var sizeAfterDel = (int)(copiedDataStart + copiedDataEnd);
                    using (var newVal = _parent.DirectAdd(_nestedValueKey, sizeAfterDel))
                        Memory.Copy(newVal.Ptr, tmp.TempPagePointer, sizeAfterDel);

                    _dataSize -= reader.Length - sizeAfterDel;
                    break;
                }
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct ResultHeader
        {
            [FieldOffset(0)]
            public long Id;

            [FieldOffset(8)]
            public ushort Size;
        }

        public int SizeAfterAdding(BlittableJsonReaderObject item)
        {
            return _dataSize + item.Size + sizeof(ResultHeader);
        }
    }
}