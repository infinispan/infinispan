ConfigurationBuilder builder = new ConfigurationBuilder();
builder.addServer()
         .host("127.0.0.1")
         .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
       .addServer()
         .host("192.0.2.0")
         .port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
       .security().authentication()
         .username("username")
         .password("changeme")
         .realm("default")
         .saslMechanism("SCRAM-SHA-512");
RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build());
