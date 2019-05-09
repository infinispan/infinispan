package org.infinispan.server;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.ServerTestConfiguration;
import org.infinispan.server.test.ServerTestMethodConfiguration;
import org.infinispan.server.test.ServerTestMethodRule;
import org.infinispan.server.test.ServerTestRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@ServerTestConfiguration(configurationFile = "server.xml")
public class ClusteredServerTest {

   @ClassRule
   public static ServerTestRule serverTestRule = new ServerTestRule();

   @Rule
   public ServerTestMethodRule serverTestMethodRule = new ServerTestMethodRule(serverTestRule);

   @Test
   @ServerTestMethodConfiguration
   public void testCluster() {
      RemoteCache<String, String> cache = serverTestMethodRule.getHotRodCache(CacheMode.DIST_SYNC);
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
