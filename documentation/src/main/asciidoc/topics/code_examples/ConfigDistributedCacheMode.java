cacheManager.defineConfiguration("dist", new ConfigurationBuilder()
      .clustering()
      .cacheMode(CacheMode.DIST_SYNC)
      .hash().numOwners(2)
      .build()
);
