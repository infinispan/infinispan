package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.InfinispanServerRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredTests.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testOperations() {
      RemoteCache<String, String> cache = SERVER_TEST.getHotRodCache(CacheMode.DIST_SYNC);
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
