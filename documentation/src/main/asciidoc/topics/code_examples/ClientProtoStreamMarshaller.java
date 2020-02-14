ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
    .host("127.0.0.1")
    .port(11222)
   .addContextInitializers(new LibraryInitializerImpl(), new AnotherExampleSciImpl())
   .build();
RemoteCacheManager rcm = new RemoteCacheManager(builder);
