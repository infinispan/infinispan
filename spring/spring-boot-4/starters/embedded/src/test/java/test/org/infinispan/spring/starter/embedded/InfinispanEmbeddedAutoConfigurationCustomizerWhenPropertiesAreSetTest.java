package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurationCustomizer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class,
              InfinispanEmbeddedAutoConfigurationCustomizerWhenPropertiesAreSetTest.TestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      }
)
@TestPropertySource(properties = "infinispan.embedded.config-xml=infinispan-test-conf.xml")
public class InfinispanEmbeddedAutoConfigurationCustomizerWhenPropertiesAreSetTest {

   private static final String CLUSTER_NAME = Util.threadLocalRandomUUID().toString();

   @Autowired
   EmbeddedCacheManager defaultCacheManager;

   @Test
   public void testCacheManagerXmlConfig() {
      assertThat(defaultCacheManager.getCacheManagerConfiguration().transport().clusterName()).isEqualTo(CLUSTER_NAME);
      assertThat(defaultCacheManager.getCacheNames()).isEqualTo(Collections.singleton("default-local"));

      final GlobalConfiguration globalConfiguration = defaultCacheManager.getCacheManagerConfiguration();
      assertThat(globalConfiguration.statistics()).isTrue();
      assertThat(globalConfiguration.jmx().domain()).isEqualTo("properties.test.spring.infinispan");
      assertThat(globalConfiguration.serialization().marshaller()).isInstanceOf(JavaSerializationMarshaller.class);

      final Configuration defaultCacheConfiguration = defaultCacheManager.getDefaultCacheConfiguration();
      assertThat(defaultCacheConfiguration.memory().maxCount()).isEqualTo(2000L);
   }

   @org.springframework.context.annotation.Configuration
   static class TestConfiguration {
      @Bean
      public InfinispanGlobalConfigurationCustomizer globalCustomizer() {
         return builder -> builder.transport().clusterName(CLUSTER_NAME).jmx().disable();
      }
   }
}
