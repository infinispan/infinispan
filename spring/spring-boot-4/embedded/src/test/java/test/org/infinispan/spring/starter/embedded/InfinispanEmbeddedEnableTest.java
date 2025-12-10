package test.org.infinispan.spring.starter.embedded;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration.DEFAULT_CACHE_MANAGER_QUALIFIER;

import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.embedded.metrics.InfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import test.org.infinispan.spring.starter.embedded.testconfiguration.InfinispanCacheTestConfiguration;

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class,
            InfinispanCacheTestConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.embedded.enabled=true",
            "infinispan.embedded.caching.enabled=true"
      }
)
public class InfinispanEmbeddedEnableTest {

   @Autowired
   private ListableBeanFactory beanFactory;

   @Test
   public void testDefaultClient() {
      assertThat(beanFactory.containsBeanDefinition(DEFAULT_CACHE_MANAGER_QUALIFIER)).isTrue();
      assertThat(beanFactory.containsBeanDefinition(InfinispanCacheMeterBinderProvider.NAME)).isTrue();
   }
}
