﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Processors.Batches;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedBatchHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/bulk_docs", "POST")]
        public async Task BulkDocs()
        {
            using (var processor = new ShardedBatchHandlerProcessorForBulkDocs(this))
                await processor.ExecuteAsync();
        }
    }

    public class SingleNodeShardedBatchCommand : ShardedCommand
    {
        private readonly JsonOperationContext _context;
        private readonly List<Stream> _commands = new List<Stream>();
        private readonly List<int> _positionInResponse = new List<int>();

        private List<Stream> _attachmentStreams;
        private HashSet<Stream> _uniqueAttachmentStreams;
        private readonly TransactionMode _mode = TransactionMode.SingleNode;
        private readonly IDisposable _returnCtx;
        public JsonOperationContext Context => _context;

        public SingleNodeShardedBatchCommand(ShardedDatabaseRequestHandler handler, JsonContextPool pool) :
            base(handler, Commands.Headers.None)
        {
            _returnCtx = pool.AllocateOperationContext(out _context);
        }

        public void AddCommand(SingleShardedCommand command)
        {
            _commands.Add(command.CommandStream);
            _positionInResponse.Add(command.PositionInResponse);

            if (command.AttachmentStream != null)
            {
                var stream = command.AttachmentStream;
                if (_attachmentStreams == null)
                {
                    _attachmentStreams = new List<Stream>();
                    _uniqueAttachmentStreams = new HashSet<Stream>();
                }

                if (_uniqueAttachmentStreams.Add(stream) == false)
                    PutAttachmentCommandHelper.ThrowStreamWasAlreadyUsed();
                _attachmentStreams.Add(stream);
            }
        }

        public void AssembleShardedReply(object[] reply)
        {
            Result.TryGet(nameof(BatchCommandResult.Results), out BlittableJsonReaderArray partialResult);
            var count = 0;
            foreach (var o in partialResult.Items)
            {
                var positionInResult = _positionInResponse[count++];
                reply[positionInResult] = o;
            }
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = base.CreateRequest(ctx, node, out url);

            request.Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();
                    await writer.WriteArrayAsync("Commands", _commands);
                    if (_mode == TransactionMode.ClusterWide)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(TransactionMode));
                        writer.WriteString(nameof(TransactionMode.ClusterWide));
                    }

                    writer.WriteEndObject();
                }
            });

            if (_attachmentStreams != null && _attachmentStreams.Count > 0)
            {
                var multipartContent = new MultipartContent { request.Content };
                foreach (var stream in _attachmentStreams)
                {
                    PutAttachmentCommandHelper.PrepareStream(stream);
                    var streamContent = new AttachmentStreamContent(stream, CancellationToken);
                    streamContent.Headers.TryAddWithoutValidation("Command-Type", "AttachmentStream");
                    multipartContent.Add(streamContent);
                }
                request.Content = multipartContent;
            }

            return request;
        }

        public override bool IsReadRequest => false;

        public void Dispose()
        {
            foreach (var command in _commands)
                command?.Dispose();

            if (_uniqueAttachmentStreams != null)
            {
                foreach (var uniqueAttachmentStream in _uniqueAttachmentStreams)
                    uniqueAttachmentStream?.Dispose();
            }

            Result?.Dispose();

            _returnCtx?.Dispose();
        }
    }

    public class SingleShardedCommand
    {
        public int ShardNumber;
        public Stream AttachmentStream;
        public Stream CommandStream;
        public int PositionInResponse;
    }

    public class ShardedBatchCommand : BatchHandler.IBatchCommand, IEnumerable<SingleShardedCommand>
    {
        private readonly TransactionOperationContext _context;
        private readonly ShardedDatabaseContext _databaseContext;

        public List<ShardedBatchCommandBuilder.BufferedCommand> BufferedCommands;
        public ArraySegment<BatchRequestParser.CommandData> ParsedCommands;
        public List<Stream> AttachmentStreams;

        public HashSet<string> ModifiedCollections { get; set; }

        public string LastChangeVector { get; set; }

        public long LastTombstoneEtag { get; set; }

        public bool IsClusterTransaction { get; set; }

        internal ShardedBatchCommand(TransactionOperationContext context, ShardedDatabaseContext databaseContext)
        {
            _context = context;
            _databaseContext = databaseContext;
        }

        public IEnumerator<SingleShardedCommand> GetEnumerator()
        {
            var streamPosition = 0;
            var positionInResponse = 0;
            for (var index = 0; index < ParsedCommands.Count; index++)
            {
                var cmd = ParsedCommands[index];
                var bufferedCommand = BufferedCommands[index];

                if (cmd.Type == CommandType.BatchPATCH)
                {
                    var idsByShard = new Dictionary<int, List<(string Id, string ChangeVector)>>();
                    foreach (var cmdId in cmd.Ids)
                    {
                        if (!(cmdId is BlittableJsonReaderObject bjro))
                            throw new InvalidOperationException();

                        if (bjro.TryGet(nameof(ICommandData.Id), out string id) == false)
                            throw new InvalidOperationException();

                        bjro.TryGet(nameof(ICommandData.ChangeVector), out string expectedChangeVector);

                        var shardId = _databaseContext.GetShardNumber(_context, id);
                        if (idsByShard.TryGetValue(shardId, out var list) == false)
                        {
                            list = new List<(string Id, string ChangeVector)>();
                            idsByShard.Add(shardId, list);
                        }
                        list.Add((id, expectedChangeVector));
                    }

                    foreach (var kvp in idsByShard)
                    {
                        yield return new SingleShardedCommand
                        {
                            ShardNumber = kvp.Key,
                            CommandStream = bufferedCommand.ModifyBatchPatchStream(kvp.Value),
                            PositionInResponse = positionInResponse
                        };
                    }

                    positionInResponse++;
                    continue;
                }

                var shard = _databaseContext.GetShardNumber(_context, cmd.Id);
                var commandStream = bufferedCommand.CommandStream;
                var stream = cmd.Type == CommandType.AttachmentPUT ? AttachmentStreams[streamPosition++] : null;

                if (bufferedCommand.IsIdentity)
                {
                    commandStream = bufferedCommand.ModifyIdentityStream(cmd.Id);
                }

                yield return new SingleShardedCommand
                {
                    ShardNumber = shard,
                    AttachmentStream = stream,
                    CommandStream = commandStream,
                    PositionInResponse = positionInResponse++
                };
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
        }
    }

    public class ShardedBatchCommandBuilder : BatchRequestParser.AbstractBatchCommandBuilder<ShardedBatchCommand, TransactionOperationContext>
    {
        public List<Stream> Streams;
        public List<BufferedCommand> BufferedCommands = new();

        private readonly bool _encrypted;
        private readonly ShardedDatabaseContext _databaseContext;

        public ShardedBatchCommandBuilder(ShardedDatabaseRequestHandler handler) :
            base(handler, handler.DatabaseContext.DatabaseName, handler.DatabaseContext.IdentityPartsSeparator)
        public ShardedBatchCommandBuilder(ShardedRequestHandler handler) :
            base(handler, handler.ShardedContext.DatabaseName, handler.ShardedContext.IdentitySeparator, BatchRequestParser.Instance)
        {
            _databaseContext = handler.DatabaseContext;
            _encrypted = handler.DatabaseContext.Encrypted;
        }

        public override async Task SaveStream(JsonOperationContext context, Stream input)
        {
            Streams ??= new List<Stream>();
            var attachment = GetServerTempFile("sharded").StartNewStream();
            await input.CopyToAsync(attachment, Handler.AbortRequestToken);
            await attachment.FlushAsync(Handler.AbortRequestToken);
            Streams.Add(attachment);
        }

        public StreamsTempFile GetServerTempFile(string prefix)
        {
            var name = $"{_databaseContext.DatabaseName}.attachment.{Guid.NewGuid():N}.{prefix}";
            var tempPath = ServerStore._env.Options.DataPager.Options.TempPath.Combine(name);

            return new StreamsTempFile(tempPath.FullPath, _encrypted);
        }

        public override async Task<BatchRequestParser.CommandData> ReadCommand(
            JsonOperationContext ctx,
            Stream stream, JsonParserState state,
            UnmanagedJsonParser parser,
            JsonOperationContext.MemoryBuffer buffer,
            BlittableMetadataModifier modifier,
            CancellationToken token)
        {
            var ms = new MemoryStream();
            try
            {
                var bufferedCommand = new BufferedCommand { CommandStream = ms };
                var result = await BatchRequestParser.ReadAndCopySingleCommand(ctx, stream, state, parser, buffer, bufferedCommand, modifier, token);
                bufferedCommand.IsIdentity = IsIdentityCommand(ref result);
                BufferedCommands.Add(bufferedCommand);
                return result;
            }
            catch
            {
                await ms.DisposeAsync();
                throw;
            }
        }

        public override async ValueTask<ShardedBatchCommand> GetCommandAsync(TransactionOperationContext context)
        {
            await ExecuteGetIdentitiesAsync();

            return new ShardedBatchCommand(context, _databaseContext)
            {
                ParsedCommands = Commands,
                BufferedCommands = BufferedCommands,
                AttachmentStreams = Streams,
                IsClusterTransaction = IsClusterTransactionRequest
            };
        }

        public class BufferedCommand
        {
            public MemoryStream CommandStream;
            public bool IsIdentity;
            public bool IsBatchPatch;

            // for identities we should replace the id and the change vector
            public int IdStartPosition;
            public int ChangeVectorPosition;
            public int IdLength;

            // for batch patch command we need to replace on to the relevant ids
            public int IdsStartPosition;
            public int IdsEndPosition;

            public MemoryStream ModifyIdentityStream(string newId)
            {
                if (IsIdentity == false)
                    throw new InvalidOperationException("Must be an identity");

                using (CommandStream)
                {
                    var modifier = new IdentityCommandModifier(IdStartPosition, IdLength, ChangeVectorPosition, newId);
                    return modifier.Rewrite(CommandStream);
                }
            }

            public MemoryStream ModifyBatchPatchStream(List<(string Id, string ChangeVector)> list)
            {
                if (IsBatchPatch == false)
                    throw new InvalidOperationException("Must be batch patch");

                var modifier = new PatchCommandModifier(IdsStartPosition, IdsEndPosition - IdsStartPosition, list);
                return modifier.Rewrite(CommandStream);
            }

            public interface IItemModifier
            {
                public void Validate();
                public int GetPosition();
                public int GetLength();
                public byte[] NewValue();
            }

            public class PatchModifier : IItemModifier
            {
                public List<(string Id, string ChangeVector)> List;
                public int IdsStartPosition;
                public int IdsLength;

                public void Validate()
                {
                    if (List == null || List.Count == 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Ids");

                    if (IdsStartPosition <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Ids position");

                    if (IdsLength <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Ids length");
                }

                public int GetPosition() => IdsStartPosition;

                public int GetLength() => IdsLength;

                public byte[] NewValue()
                {
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(ctx))
                    {
                        builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                        builder.StartArrayDocument();

                        builder.StartWriteArray();
                        foreach (var item in List)
                        {
                            builder.StartWriteObject();
                            builder.WritePropertyName(nameof(ICommandData.Id));
                            builder.WriteValue(item.Id);
                            if (item.ChangeVector != null)
                            {
                                builder.WritePropertyName(nameof(ICommandData.ChangeVector));
                                builder.WriteValue(item.ChangeVector);
                            }
                            builder.WriteObjectEnd();
                        }
                        builder.WriteArrayEnd();
                        builder.FinalizeDocument();

                        var reader = builder.CreateArrayReader();
                        return Encoding.UTF8.GetBytes(reader.ToString());
                    }
                }
            }

            public class ChangeVectorModifier : IItemModifier
            {
                public int ChangeVectorPosition;

                public void Validate()
                {
                    if (ChangeVectorPosition <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Change vector position");
                }

                public int GetPosition() => ChangeVectorPosition;
                public int GetLength() => 4; // null
                public byte[] NewValue() => Empty;

                private static readonly byte[] Empty = Encoding.UTF8.GetBytes("\"\"");
            }

            public class IdModifier : IItemModifier
            {
                public int IdStartPosition;
                public int IdLength;

                public string NewId;

                public void Validate()
                {
                    if (IdStartPosition <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Id position");

                    if (IdLength <= 0)
                        BufferedCommandModifier.ThrowArgumentMustBePositive("Id length");
                }

                public int GetPosition() => IdStartPosition;
                public int GetLength() => IdLength;
                public byte[] NewValue() => Encoding.UTF8.GetBytes(NewId);
            }

            public class PatchCommandModifier : BufferedCommandModifier
            {
                public PatchCommandModifier(int idsStartPosition, int idsLength, List<(string Id, string ChangeVector)> list)
                {
                    Items = new IItemModifier[1];
                    Items[0] = new PatchModifier
                    {
                        List = list,
                        IdsLength = idsLength,
                        IdsStartPosition = idsStartPosition
                    };
                }
            }

            public class IdentityCommandModifier : BufferedCommandModifier
            {
                public IdentityCommandModifier(int idStartPosition, int idLength, int changeVectorPosition, string newId)
                {
                    Items = new IItemModifier[2];

                    var idModifier = new IdModifier
                    {
                        IdLength = idLength,
                        IdStartPosition = idStartPosition,
                        NewId = newId
                    };
                    var cvModifier = new ChangeVectorModifier
                    {
                        ChangeVectorPosition = changeVectorPosition
                    };

                    if (changeVectorPosition < idStartPosition)
                    {
                        Items[0] = cvModifier;
                        Items[1] = idModifier;
                    }
                    else
                    {
                        Items[1] = cvModifier;
                        Items[0] = idModifier;
                    }
                }
            }

            public abstract class BufferedCommandModifier
            {
                protected IItemModifier[] Items;


                public MemoryStream Rewrite(MemoryStream source)
                {
                    EnsureInitialized();

                    var offset = 0;
                    var dest = new MemoryStream();
                    try
                    {
                        source.Position = 0;

                        var sourceBuffer = source.GetBuffer();

                        foreach (var item in Items)
                        {
                            offset = WriteRemaining(item.GetPosition());
                            dest.Write(item.NewValue());
                            offset += item.GetLength();
                        }

                        // copy the rest
                        source.Position = offset;
                        source.CopyTo(dest);

                        int WriteRemaining(int upto)
                        {
                            var remaining = upto - offset;
                            if (remaining < 0)
                                throw new InvalidOperationException();

                            if (remaining > 0)
                            {
                                dest.Write(sourceBuffer, offset, remaining);
                                offset += remaining;
                            }

                            return offset;
                        }
                    }
                    catch
                    {
                        dest.Dispose();
                        throw;
                    }

                    return dest;
                }

                private void EnsureInitialized()
                {
                    if (Items == null || Items.Length == 0)
                        throw new InvalidOperationException();

                    foreach (var item in Items)
                    {
                        item.Validate();
                    }
                }

                public static void ThrowArgumentMustBePositive(string name)
                {
                    throw new ArgumentException($"{name} must be positive");
                }
            }
        }
    }
}
