package org.infinispan.server;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.ServerTestMethodConfiguration;
import org.infinispan.server.test.ServerTestMethodRule;
import org.infinispan.server.test.ServerTestConfiguration;
import org.infinispan.server.test.ServerTestRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@ServerTestConfiguration
public class ClusteredServerTest {

   @ClassRule
   public static ServerTestRule serverTestRule = new ServerTestRule();

   @Rule
   public ServerTestMethodRule serverTestMethodRule = new ServerTestMethodRule(serverTestRule);

   @Test
   @ServerTestMethodConfiguration
   public void testCluster() {
      RemoteCacheManager client = serverTestRule.hotRodClient();
      RemoteCache<String, String> cache = client.getCache();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testCluster2() {
      RestClient restClient = serverTestRule.restClient();
      restClient.put("k2", "v2");
      assertEquals("v2", restClient.get("k2"));
   }
}
