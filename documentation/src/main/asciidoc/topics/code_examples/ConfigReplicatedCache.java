cacheManager.defineConfiguration("repl", new ConfigurationBuilder()
      .clustering()
      .cacheMode(CacheMode.REPL_SYNC)
      .build()
);
