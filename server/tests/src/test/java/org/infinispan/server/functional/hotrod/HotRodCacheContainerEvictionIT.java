package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
}
