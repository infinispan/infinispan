package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanGlobalConfigurer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import test.org.infinispan.spring.starter.embedded.testconfiguration.GlobalConfigurerJmxDisabledConfiguration;
import test.org.infinispan.spring.starter.embedded.testconfiguration.InfinispanCacheConfigurationBaseTestConfiguration;
import test.org.infinispan.spring.starter.embedded.testconfiguration.InfinispanCacheConfigurationTestConfiguration;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class,
            InfinispanCacheConfigurationBaseTestConfiguration.class,
            InfinispanCacheConfigurationTestConfiguration.class,
            InfinispanGlobalConfigurer.class,
            GlobalConfigurerJmxDisabledConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off"
      }
)
public class InfinispanEmbeddedAutoConfigurationIntegrationConfigurationTest {

   @Autowired
   EmbeddedCacheManager manager;

   @Test
   public void testConfiguration() {
      assertThat(manager.getCacheNames()).contains("base-cache", "small-cache", "large-cache");
      assertThat(manager.getCacheConfiguration("base-cache").memory().maxCount()).isEqualTo(500L);
      assertThat(manager.getCacheConfiguration("small-cache").memory().maxCount()).isEqualTo(1000L);
      assertThat(manager.getCacheConfiguration("large-cache").memory().maxCount()).isEqualTo(2000L);
   }
}
