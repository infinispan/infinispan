ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .statistics()
    .enable() <1>
    .jmxDomain("org.infinispan") <2>
   .addServer()
    .host("127.0.0.1")
    .port(11222);
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
