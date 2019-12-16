ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
      .host("127.0.0.1")
      .port(11222)
   .security()
      .ssl()
         .sniHostName("myservername")
         // TrustStore is a KeyStore which contains part of the server certificate chain (e.g. the CA Root public cert)
         .trustStoreFileName("/path/to/truststore")
         .trustStorePassword("truststorepassword".toCharArray());
RemoteCache<String, String> cache = remoteCacheManager.getCache("secured");
