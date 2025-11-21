package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.testcontainers.InfinispanContainer;
import org.junit.jupiter.api.Test;

public class VersionsInteropTest {

   @Test
   public void testOlderVersion() {
      // Reproducer for ISPN-14018.
      // We start a server with an older version than the client, they should negotiate the protocol correctly.
      try (InfinispanContainer container = new InfinispanContainer("quay.io/infinispan/server:13.0")) {
         container.start();
         try (RemoteCacheManager cacheManager = new RemoteCacheManager(container.getConnectionURI())) {
            RemoteCache<Object, Object> testCache = cacheManager.administration().getOrCreateCache("test", DefaultTemplate.DIST_SYNC);
            testCache.put("key", "value");
            assertEquals("value", testCache.get("key"));
         }
      }
   }
}
