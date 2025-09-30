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

@SpringBootTest(
      classes = {
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.embedded.enabled=false",
            "infinispan.embedded.caching.enabled=true"
      }
)
public class InfinispanEmbeddedDisableTest {

   @Autowired
   private ListableBeanFactory beanFactory;

   @Test
   public void testDefaultClient() {
      assertThat(beanFactory.containsBeanDefinition(DEFAULT_CACHE_MANAGER_QUALIFIER)).isFalse();
      assertThat(beanFactory.containsBeanDefinition(InfinispanCacheMeterBinderProvider.NAME)).isFalse();
   }
}
