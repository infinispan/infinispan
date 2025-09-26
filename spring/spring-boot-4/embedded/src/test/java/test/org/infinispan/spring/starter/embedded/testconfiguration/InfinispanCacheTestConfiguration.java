package test.org.infinispan.spring.starter.embedded.testconfiguration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.spring.starter.embedded.InfinispanCacheConfigurer;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfinispanCacheTestConfiguration {

   public static final String TEST_CLUSTER = "TEST_CLUSTER";
   public static final String TEST_CACHE_NAME = "test-simple-cache";
   public static final String TEST_GLOBAL_JMX_DOMAIN = "test.infinispan";

   @Bean
   public InfinispanCacheConfigurer cacheConfigurer() {
      return cacheManager -> {
         final org.infinispan.configuration.cache.ConfigurationBuilder testCacheBuilder = new ConfigurationBuilder();

         testCacheBuilder.simpleCache(true)
               .memory()
               .storage(StorageType.HEAP)
               .whenFull(EvictionStrategy.MANUAL);

         testCacheBuilder.statistics().enable();

         cacheManager.defineConfiguration(TEST_CACHE_NAME, testCacheBuilder.build());
      };
   }

   @Bean
   public InfinispanGlobalConfigurer globalConfigurer() {
      return () -> {
         final GlobalConfiguration globalConfiguration = new GlobalConfigurationBuilder()
               .transport().clusterName(TEST_CLUSTER)
               .jmx().domain(TEST_GLOBAL_JMX_DOMAIN).enable()
               .build();

         return globalConfiguration;
      };
   }
}
