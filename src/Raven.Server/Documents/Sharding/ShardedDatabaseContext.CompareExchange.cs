﻿using JetBrains.Annotations;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedCompareExchangeStorage CompareExchangeStorage;

    public class ShardedCompareExchangeStorage : AbstractCompareExchangeStorage
    {
        public ShardedCompareExchangeStorage([NotNull] ShardedDatabaseContext context) 
            : base(context.ServerStore)
        {
        }
    }
}