ConfigurationBuilder clientBuilder = ...; // <1>
MeterRegistry registry = ...; // <2>
MicrometerHotRodMetricRegistry.Builder metricsBuilder = new MicrometerHotRodMetricRegistry.Builder(registry) // <3>
   .withHistograms(histograms) // <4>
   .withPrefix(prefix) // <5>
   .withTag("my-tag", "my-value"); // <6>
HotRodMetricRegistry registry = metricsBuilder.build();
clientBuilder.withMetricRegistry(registry);