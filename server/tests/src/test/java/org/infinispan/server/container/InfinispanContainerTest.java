package org.infinispan.server.container;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.testcontainers.InfinispanContainer;
import org.junit.jupiter.api.Test;

public class InfinispanContainerTest {
   @Test
   public void testContainer() {
      try (InfinispanContainer container = new InfinispanContainer()) {
         container.start();
         try (RemoteCacheManager cacheManager = new RemoteCacheManager(container.getConnectionURI())) {
            RemoteCache<Object, Object> testCache = cacheManager.administration().getOrCreateCache("test", DefaultTemplate.DIST_SYNC);
            testCache.put("key", "value");
            assertEquals("value", testCache.get("key"));
         }
      }
   }
}
