package org.infinispan.hotrod;

import static org.infinispan.hotrod.AwaitAssertions.assertAwaitEquals;
import static org.infinispan.hotrod.AwaitAssertions.await;
import static org.infinispan.hotrod.HotRodServerExtension.builder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;

import org.infinispan.api.Infinispan;
import org.infinispan.api.async.AsyncCaches;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 14.0
 **/
public class HotRodAsyncCachesTest {

   @RegisterExtension
   static HotRodServerExtension server = builder().build();

   @Test
   public void testCaches() {
      try (Infinispan infinispan = server.getClient()) {
         AsyncCaches caches = infinispan.async().caches();
         assertAwaitEquals(new HashSet<>(Arrays.asList("default", "testCaches")), caches.names());
         assertEquals("testCaches", await(caches.get("testCaches")).name());
      }
   }
}
