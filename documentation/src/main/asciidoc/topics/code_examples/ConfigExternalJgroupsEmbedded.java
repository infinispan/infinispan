DefaultCacheManager cacheManager = new DefaultCacheManager(
      GlobalConfigurationBuilder.defaultClusteredBuilder()
            .transport().nodeName(nodeName).addProperty("configurationFile", "jgroups.xml")
            .build()
);
