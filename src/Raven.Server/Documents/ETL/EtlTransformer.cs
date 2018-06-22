﻿using System;
using System.Collections.Generic;
using Jint.Native;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public abstract class EtlTransformer<TExtracted, TTransformed> : IDisposable where TExtracted : ExtractedItem
    {
        public DocumentDatabase Database { get; }
        protected readonly DocumentsOperationContext Context;
        private readonly ScriptRunnerCache.Key _key;
        protected ScriptRunner.SingleRun SingleRun;

        protected TExtracted Current;
        private ScriptRunner.ReturnRun _returnRun;

        protected EtlTransformer(DocumentDatabase database, DocumentsOperationContext context,
            ScriptRunnerCache.Key key)
        {
            Database = database;
            Context = context;
            _key = key;
        }

        public virtual void Initalize()
        {
            _returnRun = Database.Scripts.GetScriptRunner(_key, true, out SingleRun);
            if (SingleRun == null)
                return;

            SingleRun.ScriptEngine.SetValue(Transformation.LoadTo, new ClrFunctionInstance(SingleRun.ScriptEngine, LoadToFunctionTranslator));

            for (var i = 0; i < LoadToDestinations.Length; i++)
            {
                var collection = LoadToDestinations[i];
                var clrFunctionInstance = new ClrFunctionInstance(SingleRun.ScriptEngine, (value, values) => LoadToFunctionTranslator(collection, value, values));
                SingleRun.ScriptEngine.SetValue(Transformation.LoadTo + collection, clrFunctionInstance);
            }

            SingleRun.ScriptEngine.SetValue(Transformation.LoadAttachment, new ClrFunctionInstance(SingleRun.ScriptEngine, LoadAttachment));

            SingleRun.ScriptEngine.SetValue(Transformation.LoadCounter, new ClrFunctionInstance(SingleRun.ScriptEngine, LoadCounter));

            SingleRun.ScriptEngine.SetValue("getAttachments", new ClrFunctionInstance(SingleRun.ScriptEngine, GetAttachments));

            SingleRun.ScriptEngine.SetValue("hasAttachment", new ClrFunctionInstance(SingleRun.ScriptEngine, HasAttachment));

            SingleRun.ScriptEngine.SetValue("getCounters", new ClrFunctionInstance(SingleRun.ScriptEngine, GetCounters));

            SingleRun.ScriptEngine.SetValue("hasCounter", new ClrFunctionInstance(SingleRun.ScriptEngine, HasCounter));
        }

        private JsValue LoadToFunctionTranslator(JsValue self, JsValue[] args)
        {
            if (args.Length != 2)
                ThrowInvalidSriptMethodCall("loadTo(name, obj) must be called with exactly 2 parameters");
            if (args[0].IsString() == false)
                ThrowInvalidSriptMethodCall("loadTo(name, obj) first argument must be a string");
            if (args[1].IsObject() == false)
                ThrowInvalidSriptMethodCall("loadTo(name, obj) second argument must be an object");

            using (var result = new ScriptRunnerResult(SingleRun, args[1].AsObject()))
            {
                LoadToFunction(args[0].AsString(), result);

                return result.Instance;
            }
        }

        private JsValue LoadToFunctionTranslator(string name, JsValue self, JsValue[] args)
        {
            if (args.Length != 1)
                ThrowInvalidSriptMethodCall($"loadTo{name}(obj) must be called with exactly 1 parameter");

            if (args[0].IsObject() == false)
                ThrowInvalidSriptMethodCall($"loadTo{name}(obj) argument must be an object");

            using (var result = new ScriptRunnerResult(SingleRun, args[0].AsObject()))
            {
                LoadToFunction(name, result);

                return result.Instance;
            }
        }

        protected abstract void AddLoadedAttachment(JsValue reference, string name, Attachment attachment);

        protected abstract void AddLoadedCounter(JsValue reference, string name, long value);

        private JsValue LoadAttachment(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidSriptMethodCall($"{Transformation.LoadAttachment}(name) must have a single string argument");

            var attachmentName = args[0].AsString();
            JsValue loadAttachmentReference = (JsValue)Transformation.AttachmentMarker + attachmentName;

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) == DocumentFlags.HasAttachments)
            {
                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(Context, Current.DocumentId, attachmentName, AttachmentType.Document, null);

                if (attachment == null)
                    ThrowNoSuchAttachment(Current.DocumentId, attachmentName);

                AddLoadedAttachment(loadAttachmentReference, attachmentName, attachment);
            }
            else
            {
                ThrowNoAttachments(Current.DocumentId, attachmentName);
            }

            return loadAttachmentReference;
        }

        private JsValue LoadCounter(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidSriptMethodCall($"{Transformation.LoadCounter}(name) must have a single string argument");

            var counterName = args[0].AsString();
            JsValue loadCounterReference = (JsValue)Transformation.CounterMarker + counterName;

            if ((Current.Document.Flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                var value = Database.DocumentsStorage.CountersStorage.GetCounterValue(Context, Current.DocumentId, counterName);

                if (value == null)
                    ThrowNoSuchCounter(Current.DocumentId, counterName);

                AddLoadedCounter(loadCounterReference, counterName, value.Value);
            }
            else
            {
                ThrowNoCounters(Current.DocumentId, counterName);
            }

            return loadCounterReference;
        }

        protected static unsafe bool IsLoadAttachment(LazyStringValue value, out string attachmentName)
        {
            if (value.Length <= Transformation.AttachmentMarker.Length)
            {
                attachmentName = null;
                return false;
            }

            var buffer = value.Buffer;

            if (*(long*)buffer != 7883660417928814884 || // $attachm
                *(int*)(buffer + 8) != 796159589) // ent/
            {
                attachmentName = null;
                return false;
            }

            attachmentName = value.Substring(Transformation.AttachmentMarker.Length);

            return true;
        }

        private JsValue GetAttachments(JsValue self, JsValue[] args)
        {
            if (args.Length != 0)
                ThrowInvalidSriptMethodCall("getAttachments() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachmentsBlittableArray) == false)
            {
                return SingleRun.ScriptEngine.Array.Construct(Array.Empty<JsValue>());
            }

            var attachments = new JsValue[attachmentsBlittableArray.Length];

            for (int i = 0; i < attachmentsBlittableArray.Length; i++)
            {
                attachments[i] = (JsValue)SingleRun.Translate(Context, attachmentsBlittableArray[i]);
            }

            return SingleRun.ScriptEngine.Array.Construct(attachments);
        }

        private JsValue HasAttachment(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidSriptMethodCall("hasAttachment(name) must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasAttachments) != DocumentFlags.HasAttachments)
                return false;

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
            {
                return false;
            }

            var checkedName = args[0].AsString();

            foreach (var attachment in attachments)
            {
                var attachmentInfo = (BlittableJsonReaderObject)attachment;
                
                if (attachmentInfo.TryGet(nameof(AttachmentName.Name), out string name) && checkedName.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private JsValue GetCounters(JsValue self, JsValue[] args)
        {
            if (args.Length != 0)
                ThrowInvalidSriptMethodCall("getCounters() must be called without any argument");

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray countersArray) == false)
            {
                return SingleRun.ScriptEngine.Array.Construct(Array.Empty<JsValue>());
            }

            var counters = new JsValue[countersArray.Length];

            for (int i = 0; i < countersArray.Length; i++)
            {
                counters[i] = (JsValue)SingleRun.Translate(Context, countersArray[i]);
            }

            return SingleRun.ScriptEngine.Array.Construct(counters);
        }

        private JsValue HasCounter(JsValue self, JsValue[] args)
        {
            if (args.Length != 1 || args[0].IsString() == false)
                ThrowInvalidSriptMethodCall("hasCounter(name) must be called with one argument (string)");

            if ((Current.Document.Flags & DocumentFlags.HasCounters) != DocumentFlags.HasCounters)
                return false;

            if (Current.Document.TryGetMetadata(out var metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters) == false)
            {
                return false;
            }

            var checkedName = args[0].AsString();

            foreach (var counter in counters)
            {
                var counterName = (LazyStringValue)counter;

                if (checkedName.Equals(counterName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        protected abstract string[] LoadToDestinations { get; }

        protected abstract void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject);

        public abstract List<TTransformed> GetTransformedResults();

        public abstract void Transform(TExtracted item);

        public static void ThrowLoadParameterIsMandatory(string parameterName)
        {
            throw new ArgumentException($"{parameterName} parameter is mandatory");
        }

        protected void ThrowNoSuchAttachment(string documentId, string attachmentName)
        {
            throw new InvalidOperationException($"Document '{documentId}' doesn't have attachment named '{attachmentName}'");
        }

        protected void ThrowNoSuchCounter(string documentId, string counterName)
        {
            throw new InvalidOperationException($"Document '{documentId}' doesn't have counter named '{counterName}'");
        }

        protected void ThrowNoAttachments(string documentId, string attachmentName)
        {
            throw new InvalidOperationException(
                $"Document '{documentId}' doesn't have any attachment while the transformation tried to add '{attachmentName}'");
        }

        protected void ThrowNoCounters(string documentId, string counterName)
        {
            throw new InvalidOperationException(
                $"Document '{documentId}' doesn't have any counter while the transformation tried to add '{counterName}'");
        }

        protected static void ThrowInvalidSriptMethodCall(string message)
        {
            throw new InvalidOperationException(message);
        }

        public void Dispose()
        {
            _returnRun.Dispose();
        }
    }
}
