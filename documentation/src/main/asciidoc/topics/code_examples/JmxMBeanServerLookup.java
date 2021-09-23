GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
  .jmx().enable().mBeanServerLookup(new com.acme.MyMBeanServerLookup())
  .build();
