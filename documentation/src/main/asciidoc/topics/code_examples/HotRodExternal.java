ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
      .host("127.0.0.1")
      .port(11222)
   .security()
      .ssl()
         // TrustStore stores trusted CA certificates for the server.
         .trustStoreFileName("/path/to/truststore")
         .trustStorePassword("truststorepassword".toCharArray())
         .trustStoreType("PKCS12")
         // KeyStore stores valid client certificates.
         .keyStoreFileName("/path/to/keystore")
         .keyStorePassword("keystorepassword".toCharArray())
         .keyStoreType("PKCS12")
      .authentication()
         .saslMechanism("EXTERNAL");
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
RemoteCache<String, String> cache = remoteCacheManager.getCache("secured");
