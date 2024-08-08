package org.infinispan.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.api.Infinispan;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.junit.jupiter.api.Test;

public class EmbeddedTest {
   @Test
   public void testEmbedded() {
      try (Infinispan infinispan = Infinispan.create("infinispan:local://infinispan")) {
         SyncCache<String, String> cache = infinispan.sync().caches().create("test", new ConfigurationBuilder().build());
         cache.set("k1", "v1");
         assertEquals("v1", cache.get("k1"));
      }
   }
}
