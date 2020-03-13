ConfigurationBuilder builder = new ConfigurationBuilder();
builder
      .addServer()
      .host("127.0.0.1")
      .port(11222)
      .addContextInitializers(new LibraryInitializerImpl(), new AnotherExampleSciImpl());
RemoteCacheManager rcm = new RemoteCacheManager(builder.build());
