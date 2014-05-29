package org.infinispan.all.embedded;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;
import org.infinispan.manager.*;
import org.infinispan.*;

@Test
public class EmbeddedAllTest {

   public void testAllEmbedded() {
      EmbeddedCacheManager cm = new DefaultCacheManager();
      Cache<String, String> cache = cm.getCache();
      cache.put("key", "value");
      assertEquals("value", cache.get("key"));

   }
}
