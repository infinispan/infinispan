ConfigurationBuilder builder = new ConfigurationBuilder();
builder.clustering().cacheMode(CacheMode.DIST_SYNC)
       .statistics().enable()
       .hotKeys().enabled(true).topK(100);
