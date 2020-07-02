package test.org.infinispan.spring.starter.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.spring.starter.embedded.InfinispanCacheConfigurer;
import org.infinispan.spring.starter.embedded.InfinispanConfigurationCustomizer;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurationCustomizer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class,
            InfinispanEmbeddedAutoConfigurationCustomizerIntegrationTest.TestConfiguration.class,
      },
      properties = {
            "spring.main.banner-mode=off"
      }
)
public class InfinispanEmbeddedAutoConfigurationCustomizerIntegrationTest {
   private static final String CLUSTER_NAME = UUID.randomUUID().toString();

   @Autowired
   EmbeddedCacheManager manager;

   static CacheStartedEvent startedEvent;

   @Test
   public void testConfiguration() {
      assertThat(manager.getCacheManagerConfiguration().transport().clusterName()).isEqualTo(CLUSTER_NAME);
      assertThat(manager.getDefaultCacheConfiguration()).isNull();

      assertThat(manager.getCacheNames()).contains("small-cache");
      assertThat(manager.getCacheConfiguration("small-cache").memory().size()).isEqualTo(1000L);
      assertThat(manager.getCacheConfiguration("small-cache").memory().evictionType()).isEqualTo(EvictionType.COUNT);
      assertThat(startedEvent).isNotNull();
   }

   @Configuration
   static class TestConfiguration {

      @Listener
      public class ExampleListener {
         @CacheStarted
         public void cacheStarted(CacheStartedEvent event) {
            startedEvent = event;
         }
      }

      @Bean(name = "small-cache")
      public org.infinispan.configuration.cache.Configuration smallCache() {
         return new ConfigurationBuilder()
               .simpleCache(true)
               .memory().size(1000L)
               .memory().evictionType(EvictionType.COUNT)
               .build();
      }

      @Bean
      public InfinispanCacheConfigurer cacheConfigurer() {
         return manager -> manager.addListener(new ExampleListener());
      }

      @Bean
      public InfinispanGlobalConfigurationCustomizer globalCustomizer() {
         return builder -> builder.transport().clusterName(CLUSTER_NAME).jmx().disable();
      }

      @Bean
      public InfinispanConfigurationCustomizer configurationCustomizer() {
         return builder -> {
            builder.memory().evictionType(EvictionType.COUNT);
         };
      }
   }
}
