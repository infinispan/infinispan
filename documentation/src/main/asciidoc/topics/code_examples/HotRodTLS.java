ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
      .host("127.0.0.1")
      .port(11222)
   .security()
      .ssl()
         // Server SNI hostname.
         .sniHostName("myservername")
         // Keystore that contains the public keys for {brandname} Server.
         // Clients use the trust store to verify {brandname} Server identities.
         .trustStoreFileName("/path/to/server/truststore")
         .trustStorePassword("truststorepassword".toCharArray())
         .trustStoreType("PCKS12")
         // Keystore that contains client certificates.
         // Clients present these certificates to {brandname} Server.
         .keyStoreFileName("/path/to/client/keystore")
         .keyStorePassword("keystorepassword".toCharArray())
         .keyStoreType("PCKS12")
      .authentication()
         // Clients must use the EXTERNAL mechanism for certificate authentication.
         .saslMechanism("EXTERNAL");
