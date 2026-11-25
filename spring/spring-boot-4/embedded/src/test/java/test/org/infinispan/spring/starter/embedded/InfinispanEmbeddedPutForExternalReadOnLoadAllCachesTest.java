package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;

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
            InfinispanEmbeddedPutForExternalReadOnLoadAllCachesTest.TestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.embedded.reactive=true",
            "infinispan.embedded.put-for-external-read-on-load=true"
      }
)
public class InfinispanEmbeddedPutForExternalReadOnLoadAllCachesTest {

   static final String TEST_CACHE = "test-cache";

   @Autowired
   InfinispanEmbeddedConfigurationProperties properties;

   @Autowired
   SpringEmbeddedCacheManager springEmbeddedCacheManager;

   @Test
   public void testPropertiesAreBound() {
      assertThat(properties.isPutForExternalReadOnLoad()).isTrue();
      assertThat(properties.getPutForExternalReadOnLoadCaches()).isEmpty();
      assertThat(properties.isPutForExternalReadOnLoad(TEST_CACHE)).isTrue();
   }

   @Test
   public void testAllCachesUsePutForExternalReadOnLoad() {
      assertThat(springEmbeddedCacheManager.getCache(TEST_CACHE)).isInstanceOf(SpringEmbeddedCache.class);
      SpringEmbeddedCache cache = (SpringEmbeddedCache) springEmbeddedCacheManager.getCache(TEST_CACHE);
      assertThat(cache.isPutForExternalReadOnLoad()).isTrue();
   }

   @Test
   public void testReactiveLoadIsStored() {
      SpringEmbeddedCache cache = (SpringEmbeddedCache) springEmbeddedCacheManager.getCache(TEST_CACHE);
      // retrieve(key, loader) is only available when the reactive property is applied together with the new one
      assertThat(cache.retrieve("key", () -> CompletableFuture.completedFuture("loaded")).join()).isEqualTo("loaded");
      assertThat(cache.getNativeCache().get("key")).isEqualTo("loaded");
   }

   @org.springframework.context.annotation.Configuration
   static class TestConfiguration {
      @Bean
      public InfinispanCacheConfigurer cacheConfigurer() {
         return cacheManager -> cacheManager.defineConfiguration(TEST_CACHE, new ConfigurationBuilder().build());
      }
   }
}
