ConfigurationBuilder builder = new ConfigurationBuilder();
builder.clustering().cacheMode(CacheMode.DIST_SYNC)
         .l1()
         .lifespan(5000, TimeUnit.MILLISECONDS)
         .cleanupTaskFrequency(60000, TimeUnit.MILLISECONDS);
