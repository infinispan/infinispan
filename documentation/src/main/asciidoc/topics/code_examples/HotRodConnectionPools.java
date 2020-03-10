ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
    .host("127.0.0.1")
    .port(11222);
   .connectionPool() <1>
    .maxActive(10) <2>
    .minIdle(20); <3>
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
