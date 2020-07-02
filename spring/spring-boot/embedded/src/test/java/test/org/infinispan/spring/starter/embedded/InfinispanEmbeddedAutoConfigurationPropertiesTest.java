package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      }
)
@TestPropertySource(properties = "infinispan.embedded.config-xml=infinispan-test-conf.xml")
public class InfinispanEmbeddedAutoConfigurationPropertiesTest {

   @Autowired
   EmbeddedCacheManager defaultCacheManager;

   @Test
   public void testCacheManagerXmlConfig() {
      assertThat(defaultCacheManager.getCacheNames()).isEqualTo(Collections.singleton("default-local"));

      final GlobalConfiguration globalConfiguration = defaultCacheManager.getCacheManagerConfiguration();
      assertThat(globalConfiguration.globalJmxStatistics().enabled()).isTrue();
      assertThat(globalConfiguration.globalJmxStatistics().domain()).isEqualTo("properties.test.spring.infinispan");

      final Configuration defaultCacheConfiguration = defaultCacheManager.getDefaultCacheConfiguration();
      assertThat(defaultCacheConfiguration.memory().size()).isEqualTo(2000L);
   }
}
