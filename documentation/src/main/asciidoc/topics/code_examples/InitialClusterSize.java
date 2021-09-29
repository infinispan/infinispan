GlobalConfiguration global = GlobalConfigurationBuilder.defaultClusteredBuilder()
   .transport()
   .initialClusterSize(4)
   .initialClusterTimeout(30000, TimeUnit.MILLISECONDS);
