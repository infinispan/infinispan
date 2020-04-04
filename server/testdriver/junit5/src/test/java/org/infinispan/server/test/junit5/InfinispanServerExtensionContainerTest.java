package org.infinispan.server.test.junit5;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfinispanServerExtensionContainerTest {

   @RegisterExtension
   static InfinispanServerExtension SERVER = InfinispanServerExtensionBuilder.server();

   @Test
   public void testSingleServer() {
      RemoteCache<String, String> cache =  SERVER.hotrod()
            .withCacheMode(CacheMode.DIST_SYNC).create();

      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
