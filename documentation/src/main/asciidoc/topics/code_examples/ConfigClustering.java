Configuration config = new ConfigurationBuilder()
  .clustering()
    .cacheMode(CacheMode.DIST_SYNC)
    .l1().lifespan(25000L)
    .hash().numOwners(3)
  .build();
