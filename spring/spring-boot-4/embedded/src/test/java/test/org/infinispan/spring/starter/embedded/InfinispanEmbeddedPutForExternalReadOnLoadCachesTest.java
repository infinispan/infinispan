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
            InfinispanEmbeddedPutForExternalReadOnLoadCachesTest.TestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.embedded.put-for-external-read-on-load-caches=books,authors"
      }
)
public class InfinispanEmbeddedPutForExternalReadOnLoadCachesTest {

   static final String BOOKS = "books";
   static final String AUTHORS = "authors";
   static final String OTHER = "other";

   @Autowired
   InfinispanEmbeddedConfigurationProperties properties;

   @Autowired
   SpringEmbeddedCacheManager springEmbeddedCacheManager;

   @Test
   public void testPropertiesAreBound() {
      assertThat(properties.isPutForExternalReadOnLoad()).isFalse();
      assertThat(properties.getPutForExternalReadOnLoadCaches()).containsExactly(BOOKS, AUTHORS);
      assertThat(properties.isPutForExternalReadOnLoad(BOOKS)).isTrue();
      assertThat(properties.isPutForExternalReadOnLoad(OTHER)).isFalse();
   }

   @Test
   public void testOnlySelectedCachesUsePutForExternalReadOnLoad() {
      assertThat(springCache(BOOKS).isPutForExternalReadOnLoad()).isTrue();
      assertThat(springCache(AUTHORS).isPutForExternalReadOnLoad()).isTrue();
      assertThat(springCache(OTHER).isPutForExternalReadOnLoad()).isFalse();
   }

   private SpringEmbeddedCache springCache(String name) {
      assertThat(springEmbeddedCacheManager.getCache(name)).isInstanceOf(SpringEmbeddedCache.class);
      return (SpringEmbeddedCache) springEmbeddedCacheManager.getCache(name);
   }

   @org.springframework.context.annotation.Configuration
   static class TestConfiguration {
      @Bean
      public InfinispanCacheConfigurer cacheConfigurer() {
         return cacheManager -> {
            for (String name : new String[]{BOOKS, AUTHORS, OTHER}) {
               cacheManager.defineConfiguration(name, new ConfigurationBuilder().build());
            }
         };
      }
   }
}
