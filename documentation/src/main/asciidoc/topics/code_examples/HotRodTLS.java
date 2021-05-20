ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
      .host("127.0.0.1")
      .port(11222)
   .security()
      .ssl()
         // Server SNI hostname.
         .sniHostName("myservername")
         // Server certificate keystore.
         .trustStoreFileName("/path/to/truststore")
         .trustStorePassword("truststorepassword".toCharArray())
         .trustStoreType("PCKS12")
         // Client certificate keystore.
         .keyStoreFileName("/path/to/client/keystore")
         .keyStorePassword("keystorepassword".toCharArray())
         .keyStoreType("PCKS12");
RemoteCache<String, String> cache=remoteCacheManager.getCache("secured");
