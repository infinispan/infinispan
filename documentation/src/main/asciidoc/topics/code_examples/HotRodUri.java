ConfigurationBuilder builder = new ConfigurationBuilder();
builder.uri("hotrod://username:changeme@127.0.0.1:11222,192.0.2.0:11222?auth_realm=default&sasl_mechanism=SCRAM-SHA-512");
RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build());
