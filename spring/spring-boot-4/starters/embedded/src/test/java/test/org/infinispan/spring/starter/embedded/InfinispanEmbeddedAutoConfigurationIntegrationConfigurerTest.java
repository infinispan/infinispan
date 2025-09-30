package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurationCustomizer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import test.org.infinispan.spring.starter.embedded.testconfiguration.InfinispanCacheTestConfiguration;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class,
            InfinispanCacheTestConfiguration.class,
            InfinispanEmbeddedAutoConfigurationIntegrationConfigurerTest.TestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      }
)
public class InfinispanEmbeddedAutoConfigurationIntegrationConfigurerTest {

   private static final String JMX_TEST_DOMAIN = InfinispanEmbeddedAutoConfigurationIntegrationConfigurerTest.class.getName();

   @org.springframework.context.annotation.Configuration
   static class TestConfiguration {
      @Bean
      public InfinispanGlobalConfigurationCustomizer globalCustomizer() {
         return builder -> builder.jmx()
               .domain(JMX_TEST_DOMAIN);
      }
   }

   @Autowired
   EmbeddedCacheManager defaultCacheManager;

   @Test
   public void testWithCacheConfigurer() {
      assertThat(defaultCacheManager.getCacheNames()).containsExactlyInAnyOrder(InfinispanCacheTestConfiguration.TEST_CACHE_NAME);

      final Configuration testCacheConfiguration = defaultCacheManager.getCacheConfiguration(InfinispanCacheTestConfiguration.TEST_CACHE_NAME);
      assertThat(testCacheConfiguration.statistics().enabled()).isTrue();
      assertThat(testCacheConfiguration.memory().storage()).isEqualTo(StorageType.HEAP);
      assertThat(testCacheConfiguration.memory().whenFull()).isEqualTo(EvictionStrategy.MANUAL);
   }

   @Test
   public void testWithGlobalConfigurer() {
      final GlobalConfiguration globalConfiguration = defaultCacheManager.getCacheManagerConfiguration();

      assertThat(globalConfiguration.jmx().enabled()).isTrue();
      assertThat(globalConfiguration.jmx().domain()).isEqualTo(JMX_TEST_DOMAIN);
      assertThat(globalConfiguration.transport().clusterName()).isEqualTo(InfinispanCacheTestConfiguration.TEST_CLUSTER);
   }
}
