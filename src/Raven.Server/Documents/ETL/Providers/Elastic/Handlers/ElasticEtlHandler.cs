﻿using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Elastic.Test;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Elastic.Handlers
{
    public class ElasticEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/elastic/test", "POST", AuthorizationStatus.Operator)]
        public async Task PostScriptTest()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var dbDoc = await context.ReadForMemoryAsync(RequestBodyStream(), "TestElasticEtlScript");
                var testScript = JsonDeserializationServer.TestElasticEtlScript(dbDoc);

                var result = (ElasticEtlTestScriptResult)ElasticEtl.TestScript(testScript, Database, ServerStore, context);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                    writer.WriteObject(context.ReadObject(djv, "et/elastic/test"));
                }
            }
        }
    }
}
