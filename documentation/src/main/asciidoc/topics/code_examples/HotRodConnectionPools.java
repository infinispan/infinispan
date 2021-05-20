ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder.addServer()
               .host("127.0.0.1")
               .port(11222)
             .connectionPool()
               .maxActive(10)
               .exhaustedAction(1)
               .maxWait(1)
               .minIdle(20)
               .minEvictableIdleTime(300000)
               .maxPendingRequests(20);
RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
