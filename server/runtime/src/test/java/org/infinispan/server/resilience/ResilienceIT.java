package org.infinispan.server.resilience;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.ServerRunMode;
import org.infinispan.server.test.category.Resilience;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Category(Resilience.class)
public class ResilienceIT {

   @ClassRule
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml")
         .serverRunMode(ServerRunMode.CONTAINER));

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testUnresponsiveNode() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.socketTimeout(1000).connectionTimeout(1000).maxRetries(10).connectionPool().maxActive(1);
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.REPL_SYNC).create();

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
