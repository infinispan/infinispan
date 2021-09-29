GlobalConfiguration global = GlobalConfigurationBuilder.defaultClusteredBuilder()
   .jmx().enable()
   .domain("org.mydomain")
   .mBeanServerLookup(new com.acme.MyMBeanServerLookup());
