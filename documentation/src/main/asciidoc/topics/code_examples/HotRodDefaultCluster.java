ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder.addServers("172.20.0.11:11222; 172.20.0.12:11222")
             .addCluster("siteB")
               .addClusterNodes("172.21.0.11:11222; 172.21.0.12:11222");
RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
