package test.infinispan.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedAutoConfiguration;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedCacheManagerAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteAutoConfiguration;
import org.infinispan.spring.starter.remote.InfinispanRemoteCacheManagerAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(
      classes = {
            InfinispanRemoteAutoConfiguration.class,
            InfinispanRemoteCacheManagerAutoConfiguration.class,
            InfinispanEmbeddedAutoConfiguration.class,
            InfinispanEmbeddedCacheManagerAutoConfiguration.class
      },
      properties = {
            "spring.main.banner-mode=off",
            "infinispan.remote.server-list=127.0.0.1:6667"
      })
@DirtiesContext
public class CacheManagerTest {
   @Autowired
   private ApplicationContext context;

   @Test
   public void testRemoteCacheManager() {
      assertNotNull(context.getBean(RemoteCacheManager.class));
   }

   @Test
   public void testDefaultCacheManager() {
      assertNotNull(context.getBean(DefaultCacheManager.class));
   }
}
