﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Batches.Commands;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Batches;

public abstract class AbstractBatchCommandsReader<TBatchCommand, TOperationContext>
    where TBatchCommand : IBatchCommand
    where TOperationContext : JsonOperationContext
{
    private static readonly BatchRequestParser.CommandData[] Empty = Array.Empty<BatchRequestParser.CommandData>();

    private readonly char _identityPartsSeparator;
    private readonly BatchRequestParser _batchRequestParser;
    protected readonly RequestHandler Handler;
    private readonly string _database;
    protected ServerStore ServerStore => Handler.ServerStore;

    private int _index = -1;
    private BatchRequestParser.CommandData[] _commands = Empty;
    public ArraySegment<BatchRequestParser.CommandData> Commands => new ArraySegment<BatchRequestParser.CommandData>(_commands, 0, _index + 1);

    protected List<string> Identities;
    protected List<int> IdentityPositions;

    public bool HasIdentities => Identities != null;
    public bool IsClusterTransactionRequest;

    protected AbstractBatchCommandsReader(RequestHandler handler, string database, char identityPartsSeparator, BatchRequestParser batchRequestParser)
    {
        Handler = handler;
        _database = database;
        _identityPartsSeparator = identityPartsSeparator;
        _batchRequestParser = batchRequestParser;
    }

    private void AddIdentity(JsonOperationContext ctx, ref BatchRequestParser.CommandData command, int index)
    {
        Identities ??= new List<string>();
        IdentityPositions ??= new List<int>();

        Identities.Add(command.Id);
        IdentityPositions.Add(index);

        command.ChangeVector ??= ctx.GetLazyString("");
    }

    protected async ValueTask ExecuteGetIdentitiesAsync()
    {
        if (HasIdentities == false)
            return;

        var newIds = await ServerStore.GenerateClusterIdentitiesBatchAsync(_database, Identities, RaftIdGenerator.NewId());
        Debug.Assert(newIds.Count == Identities.Count);

        for (var index = 0; index < IdentityPositions.Count; index++)
        {
            var value = IdentityPositions[index];
            var cmd = _commands[value];

            cmd.Id = cmd.Id.Substring(0, cmd.Id.Length - 1) + _identityPartsSeparator + newIds[index];

            if (string.IsNullOrEmpty(cmd.ChangeVector) == false)
                ThrowInvalidUsageOfChangeVectorWithIdentities(cmd);

            // command it a struct so we need to copy it again
            _commands[value] = cmd;
        }
    }

    public abstract Task SaveStream(JsonOperationContext context, Stream input);

    public virtual Task<BatchRequestParser.CommandData> ReadCommand(
        JsonOperationContext ctx,
        Stream stream,
        JsonParserState state,
        UnmanagedJsonParser parser,
        JsonOperationContext.MemoryBuffer buffer,
        BlittableMetadataModifier modifier,
        CancellationToken token)
    {
        return _batchRequestParser.ReadSingleCommand(ctx, stream, state, parser, buffer, modifier, token);
    }

    public async Task BuildCommandsAsync(JsonOperationContext context, Stream stream, char separator)
    {
        var state = new JsonParserState();
        using (context.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer buffer))
        using (var parser = new UnmanagedJsonParser(context, state, "bulk_docs"))
        /* In case we have a conflict between attachment with the same name we need attachment information from metadata */
        /* we can't know from advanced if we will need this information so we save this for all batch commands */
        using (var modifier = new BlittableMetadataModifier(context, legacyImport: false, readLegacyEtag: false, DatabaseItemType.Attachments))
        {
            while (parser.Read() == false)
                await _batchRequestParser.RefillParserBuffer(stream, buffer, parser);

            if (state.CurrentTokenType != JsonParserToken.StartObject)
                BatchRequestParser.ThrowUnexpectedToken(JsonParserToken.StartObject, state);

            while (parser.Read() == false)
                await _batchRequestParser.RefillParserBuffer(stream, buffer, parser);

            if (state.CurrentTokenType != JsonParserToken.String)
                BatchRequestParser.ThrowUnexpectedToken(JsonParserToken.String, state);

            if (BatchRequestParser.GetLongFromStringBuffer(state) != 8314892176759549763) // Commands
                BatchRequestParser.ThrowUnexpectedToken(JsonParserToken.String, state);

            while (parser.Read() == false)
                await _batchRequestParser.RefillParserBuffer(stream, buffer, parser);

            if (state.CurrentTokenType != JsonParserToken.StartArray)
                BatchRequestParser.ThrowUnexpectedToken(JsonParserToken.StartArray, state);

            while (true)
            {
                while (parser.Read() == false)
                    await _batchRequestParser.RefillParserBuffer(stream, buffer, parser);

                if (state.CurrentTokenType == JsonParserToken.EndArray)
                    break;

                _index++;
                if (_index >= _commands.Length)
                {
                    _commands = BatchRequestParser.IncreaseSizeOfCommandsBuffer(_index, _commands);
                }

                var commandData = await ReadCommand(context, stream, state, parser, buffer, modifier, Handler.AbortRequestToken);

                if (commandData.Type == CommandType.PATCH)
                {
                    //TODO sharding: make it nicer
                    commandData.PatchCommand =
                        new PatchDocumentCommand(
                            context,
                            commandData.Id,
                            commandData.ChangeVector,
                            skipPatchIfChangeVectorMismatch: false,
                            (commandData.Patch, commandData.PatchArgs),
                            (commandData.PatchIfMissing, commandData.PatchIfMissingArgs),
                            commandData.CreateIfMissing,
                            isTest: false,
                            debugMode: false,
                            collectResultsNeeded: true,
                            returnDocument: commandData.ReturnDocument,
                            identityPartsSeparator: separator
                        );
                }

                if (commandData.Type == CommandType.JsonPatch)
                {
                    commandData.JsonPatchCommand = new JsonPatchCommand(
                        commandData.Id,
                        commandData.JsonPatchCommands,
                        commandData.ReturnDocument,
                        context);
                }

                if (commandData.Type == CommandType.BatchPATCH)
                {
                    //TODO sharding: make it nicer
                    commandData.PatchCommand =
                        new BatchPatchDocumentCommand(
                            context,
                            commandData.Ids,
                            skipPatchIfChangeVectorMismatch: false,
                            (commandData.Patch, commandData.PatchArgs),
                            (commandData.PatchIfMissing, commandData.PatchIfMissingArgs),
                            commandData.CreateIfMissing,
                            isTest: false,
                            debugMode: false,
                            collectResultsNeeded: true
                        );
                }

                if (IsIdentityCommand(ref commandData))
                {
                    // queue identities requests in order to send them at once to the leader (using List for simplicity)
                    AddIdentity(context, ref commandData, _index);
                }

                _commands[_index] = commandData;
            }

            if (await _batchRequestParser.IsClusterTransaction(stream, parser, buffer, state))
                IsClusterTransactionRequest = true;
        }
    }

    public static bool IsIdentityCommand(ref BatchRequestParser.CommandData commandData)
    {
        return commandData.Type == CommandType.PUT && string.IsNullOrEmpty(commandData.Id) == false && commandData.Id[^1] == '|';
    }

    public async Task ParseMultipart(JsonOperationContext context, Stream stream, string contentType, char separator)
    {
        var boundary = MultipartRequestHelper.GetBoundary(
            MediaTypeHeaderValue.Parse(contentType),
            MultipartRequestHelper.MultipartBoundaryLengthLimit);
        var reader = new MultipartReader(boundary, stream);
        for (var i = 0; i < int.MaxValue; i++)
        {
            var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
            if (section == null)
                break;

            var bodyStream = Handler.GetBodyStream(section);
            if (i == 0)
            {
                await BuildCommandsAsync(context, bodyStream, separator);
                continue;
            }

            await SaveStream(context, bodyStream);
        }
    }

    public abstract ValueTask<TBatchCommand> GetCommandAsync(TOperationContext context);

    private static void ThrowInvalidUsageOfChangeVectorWithIdentities(BatchRequestParser.CommandData commandData)
    {
        throw new InvalidOperationException($"You cannot use change vector ({commandData.ChangeVector}) " +
                                            $"when using identity in the document ID ({commandData.Id}).");
    }
}
