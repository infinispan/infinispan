GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
  .jmx()
    .cacheManagerName("SalesCacheManager")
    .mBeanServerLookup(new JBossMBeanServerLookup())
  .build();
