ConfigurationBuilder builder = new ConfigurationBuilder();
builder.clustering().cacheMode(CacheMode.DIST_SYNC)
       .partitionHandling()
       .whenSplit(PartitionHandling.DENY_READ_WRITES)
       .mergePolicy(MergePolicy.PREFERRED_NON_NULL);
