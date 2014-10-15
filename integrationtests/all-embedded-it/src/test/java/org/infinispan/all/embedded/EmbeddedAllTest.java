package org.infinispan.all.embedded;

import static org.junit.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.Test;

public class EmbeddedAllTest {

   @Test
   public void testAllEmbedded() {
      EmbeddedCacheManager cm = new DefaultCacheManager();
      try {
         Cache<String, String> cache = cm.getCache();
         cache.put("key", "value");
         assertEquals("value", cache.get("key"));
      } finally {
         cm.stop();
      }
   }
}
