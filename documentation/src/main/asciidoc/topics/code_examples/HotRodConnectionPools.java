ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
    .host("127.0.0.1")
    .port(11222)
    //Configure client connection pools.
    .connectionPool()
    //Set the maximum number of active connections per server.
    .maxActive(10)
    //Set the minimum number of idle connections
    //that must be available per server.
    .minIdle(20);
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
