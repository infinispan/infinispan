package org.infinispan.server.resilience;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.test.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Resilience;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Category(Resilience.class)
public class ResilienceIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
                                    .runMode(ServerRunMode.CONTAINER)
                                    .build();

   @Test
   public void testUnresponsiveNode() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.socketTimeout(1000).connectionTimeout(1000).maxRetries(10).connectionPool().maxActive(1);
      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.REPL_SYNC).create();

      cache.put("k1", "v1");
      assertEquals("v1", cache.get("k1"));
      SERVERS.getServerDriver().pause(0);
      Eventually.eventually("Cluster should have 1 node", () -> {
         cache.get("k1");
         return cache.getCacheTopologyInfo().getSegmentsPerServer().size() == 1;
      }, 30, 1, TimeUnit.SECONDS);
      SERVERS.getServerDriver().resume(0);
      Eventually.eventually("Cluster should have 2 nodes", () -> {
         cache.get("k1");
         return cache.getCacheTopologyInfo().getSegmentsPerServer().size() == 2;
      }, 30, 1, TimeUnit.SECONDS);
   }
}
