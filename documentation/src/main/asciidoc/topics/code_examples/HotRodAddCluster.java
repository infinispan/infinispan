ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addCluster()
    .addClusterNode("cluster-hostname", 11222); <1>
   .addServer() <2>
    .host("127.0.0.1")
    .port(11222);
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
