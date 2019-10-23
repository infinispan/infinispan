package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodCacheOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testHotRodOperations() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
      cache.remove("k1");
      assertEquals(0, cache.size());
   }
}
