GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
  .statistics().enable()
  .metrics().gauges(true).histograms(true)
  .jmx().enable()
  .build();
