package org.infinispan.server.container;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.core.InfinispanContainer;
import org.junit.Test;

public class InfinispanContainerTest {
   @Test
   public void testContainer() {
      try (InfinispanContainer container = new InfinispanContainer()) {
         container.start();
         try (RemoteCacheManager cacheManager = container.getRemoteCacheManager()) {
            RemoteCache<Object, Object> testCache = cacheManager.administration().getOrCreateCache("test", DefaultTemplate.DIST_SYNC);
            testCache.put("key", "value");
            assertEquals("value", testCache.get("key"));
         }
      }
   }
}
