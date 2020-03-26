GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
  .cacheContainer().statistics(true)
  .metrics().gauges(true).histograms(true)
  .jmx().enable()
  .build();
