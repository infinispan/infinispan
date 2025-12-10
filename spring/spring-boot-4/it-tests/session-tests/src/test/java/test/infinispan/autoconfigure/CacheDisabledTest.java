package test.infinispan.autoconfigure;

import org.infinispan.client.hotrod.RemoteCacheManager;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.cache.autoconfigure.CacheAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(
      classes = {
            CacheAutoConfiguration.class,
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class,
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "spring.cache.type=NONE",
            "infinispan.remote.server-list=127.0.0.1:6667"
      })
@DirtiesContext
public class CacheDisabledTest {
   @Autowired
   private ApplicationContext context;

   @Test
   public void testDefaultCacheManager() {
      Assertions.assertThrows(NoSuchBeanDefinitionException.class, () -> {
         context.getBean(DefaultCacheManager.class);
      });
   }

   @Test
   public void testRemoteCacheManager() {
      Assertions.assertThrows(NoSuchBeanDefinitionException.class, () -> {
         context.getBean(RemoteCacheManager.class);
      });
   }
}
