RemoteCacheManagerMetricsRegistry registry = ...;
ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder.addServer()
               .host("127.0.0.1")
               .port(11222)
             .withMetricRegistry(registry);
RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
