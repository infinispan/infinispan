ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
clientBuilder.addCluster("siteA")
               .addClusterNode("hostA1", 11222)
               .addClusterNode("hostA2", 11222)
             .addCluster("siteB")
               .addClusterNodes("hostB1:11222; hostB2:11222");
RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
