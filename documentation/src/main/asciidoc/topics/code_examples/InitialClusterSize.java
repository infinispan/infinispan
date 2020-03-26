GlobalConfiguration global = new GlobalConfigurationBuilder()
   .transport()
   .initialClusterSize(4)
   .initialClusterTimeout(30000, TimeUnit.MILLISECONDS)
   .build();
