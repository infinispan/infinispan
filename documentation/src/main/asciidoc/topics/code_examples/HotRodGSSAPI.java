LoginContext lc = new LoginContext("GssExample", new BasicCallbackHandler("krb_user", "krb_password".toCharArray()));
lc.login();
Subject clientSubject = lc.getSubject();

ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder
   .addServer()
      .host("127.0.0.1")
      .port(11222)
   .security()
      .authentication()
         .enable()
         .saslMechanism("GSSAPI")
         .clientSubject(clientSubject)
         .callbackHandler(new BasicCallbackHandler());
remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
RemoteCache<String, String> cache = remoteCacheManager.getCache("secured");
