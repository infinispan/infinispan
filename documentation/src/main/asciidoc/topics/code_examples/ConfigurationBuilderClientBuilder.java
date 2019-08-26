ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
    .addServer()
        .host("127.0.0.1")
        .port(hotrodServer.getPort())
     .security()
        .ssl()
           .enabled(sslClient)
           .sniHostName("hotrod-1") // SNI Host Name
           .trustStoreFileName("truststore.jks")
           .trustStorePassword("secret".toCharArray());
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
