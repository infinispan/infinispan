ConfigurationBuilder builder = new ConfigurationBuilder();
builder.clustering().cacheMode(CacheMode.DIST_SYNC)
       .partitionHandling()
       .whenSplit(PartitionHandling.DENY_READ_WRITES)
       .mergePolicy(new CustomMergePolicy());
