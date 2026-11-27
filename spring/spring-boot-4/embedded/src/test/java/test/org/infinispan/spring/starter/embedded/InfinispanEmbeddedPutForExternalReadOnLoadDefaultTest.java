package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCache;
import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanCacheConfigurer;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedConfigurationProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;

import test.org.infinispan.spring.starter.embedded.testconfiguration.GlobalConfigurerJmxDisabledConfiguration;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class,
            GlobalConfigurerJmxDisabledConfiguration.class,
            InfinispanEmbeddedPutForExternalReadOnLoadDefaultTest.TestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      }
)
public class InfinispanEmbeddedPutForExternalReadOnLoadDefaultTest {

   static final String TEST_CACHE = "test-cache";

   @Autowired
   InfinispanEmbeddedConfigurationProperties properties;

   @Autowired
   SpringEmbeddedCacheManager springEmbeddedCacheManager;

   @Test
   public void testPropertiesDefaults() {
      assertThat(properties.isPutForExternalReadOnLoad()).isFalse();
      assertThat(properties.getPutForExternalReadOnLoadCaches()).isEmpty();
      assertThat(properties.isPutForExternalReadOnLoad(TEST_CACHE)).isFalse();
   }

   @Test
   public void testPutForExternalReadOnLoadIsDisabledByDefault() {
      assertThat(springEmbeddedCacheManager.getCache(TEST_CACHE))
            .isInstanceOfSatisfying(SpringEmbeddedCache.class, cache -> assertThat(cache.isPutForExternalReadOnLoad()).isFalse());
   }

   @org.springframework.context.annotation.Configuration
   static class TestConfiguration {
      @Bean
      public InfinispanCacheConfigurer cacheConfigurer() {
         return cacheManager -> cacheManager.defineConfiguration(TEST_CACHE, new ConfigurationBuilder().build());
      }
   }
}
