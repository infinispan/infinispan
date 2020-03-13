ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServers("hostA1:11222; hostA2:11222")
   .addCluster("siteB")
     .addClusterNodes("hostB1:11222; hostB2:11223");
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
