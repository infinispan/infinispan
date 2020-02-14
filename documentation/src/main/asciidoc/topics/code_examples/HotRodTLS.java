ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
      .host("127.0.0.1")
      .port(11222)
   .security()
      .ssl()
         // Server SNI hostname.
         .sniHostName("myservername") <1>
         // Server certificate keystore.
         .trustStoreFileName("/path/to/truststore") <2>
         .trustStorePassword("truststorepassword".toCharArray())
         // Client certificate keystore.
         .keyStoreFileName("/path/to/client/keystore") <3>
         .keyStorePassword("keystorepassword".toCharArray());
RemoteCache<String, String> cache=remoteCacheManager.getCache("secured");
