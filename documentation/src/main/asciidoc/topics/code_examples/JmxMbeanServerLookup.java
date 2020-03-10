GlobalConfigurationBuilder globalConfigurationBuilder = ...
globalConfigurationBuilder.jmx()
    .enable()
    .mBeanServerLookup(new com.acme.MyMBeanServerLookup());
