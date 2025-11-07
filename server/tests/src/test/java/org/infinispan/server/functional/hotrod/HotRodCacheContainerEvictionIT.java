package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HotRodCacheContainerEvictionIT {

   private static final int NUM_SERVERS = 1;

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerWithEvictionContainerTest.xml")
               .numServers(NUM_SERVERS)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testSharedEvictionCount() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().evictionContainer("count-container");
      RemoteCache<String, String> first = SERVERS.hotrod().withServerConfiguration(configurationBuilder).create("first-cache");
      RemoteCache<String, String> second = SERVERS.hotrod().withServerConfiguration(configurationBuilder).create("second-cache");

      for (int i = 0; i < 10; ++i) {
         first.put("key-" + i, "value-" + i);
      }

      assertEquals(10, first.size());

      for (int i = 0; i < 3; ++i) {
         second.put("key-" + i, "value-" + i);
      }

      int firstSize = first.size();
      assertNotEquals(10, firstSize);

      int secondSize = second.size();
      assertEquals(10, firstSize + secondSize);
   }

   @Test
   public void testSharedEvictionSize() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().evictionContainer("size-container");
      RemoteCache<String, String> first = SERVERS.hotrod().withServerConfiguration(configurationBuilder).create("third-cache");
      RemoteCache<String, String> second = SERVERS.hotrod().withServerConfiguration(configurationBuilder).create("fourth-cache");

      int size = 0;
      for (; size < 10; ++size) {
         first.put("key-" + size, "value-" + size);
      }

      int failSafeInsert = 100;

      while (size == first.size()) {
         first.put("key-" + size, "value-" + size);
         if (failSafeInsert == size) {
            fail("No eviction encountered after " + failSafeInsert + " inserts!");
         }
         size++;
      }

      int finalSize = first.size();

      for (int i = 0; i < 3; ++i) {
         second.put("key-" + i, "value-" + i);
      }

      // The writes to the second cache should cause the first to evict something
      assertNotEquals(finalSize, first.size());
   }
}
