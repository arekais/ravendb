﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21173 : ClusterTestBase
{
    public RavenDB_21173(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClusterTransactions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ClusterTransaction_Failover_Shouldnt_Throw_ConcurrencyException(Options options)
    {
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 3);
        options.ReplicationFactor = 3;
        options.Server = leader;
        using var store = GetDocumentStore(options);
        var databaseName = store.Database;

        var disposeNodeTask = Task.Run(async () =>
        {
            await Task.Delay(400);
            var tag = store.GetRequestExecutor(databaseName).TopologyNodes.First().ClusterTag;
            var server = nodes.Single(n => n.ServerStore.NodeTag == tag);
            await DisposeServerAndWaitForFinishOfDisposalAsync(server);
        });
        await ProcessDocument(store, "Docs/1-A");

        await disposeNodeTask;
    }

    private async Task ProcessDocument(IDocumentStore store, string id)
    {
        using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            var doc = new Doc { Id = id };
            await session.StoreAsync(doc);
            await session.SaveChangesAsync();
        }

        for (int i = 0; i < 2000; i++)
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var doc = await session.LoadAsync<Doc>(id);
                doc.Progress = i;
                await session.SaveChangesAsync();
            }
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public int Progress { get; set; }
    }

}
