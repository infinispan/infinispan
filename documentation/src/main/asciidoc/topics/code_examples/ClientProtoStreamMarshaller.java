ConfigurationBuilder builder = new ConfigurationBuilder()
      .addServer().host("localhost").port(hotRodServer.getPort())
      .addContextInitializers(new LibraryInitializerImpl(), new AnotherExampleSciImpl())
      .build();
RemoteCacheManager rcm = new RemoteCacheManager(builder);