@Bean
public InfinispanCacheConfigurer cacheConfigurer() {
  return cacheManager -> {
     final org.infinispan.configuration.cache.Configuration config =
           new ConfigurationBuilder()
                 .jmxStatistics().enable()
                 .build();

     cacheManager.defineConfiguration("my-cache", config);
  };
}
