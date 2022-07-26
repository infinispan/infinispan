package org.infinispan.server.test.junit4;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class InfinispanRuleBasicTest {
   @ClassRule
   public static final InfinispanServerRule SERVER = InfinispanServerRuleBuilder.server();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testSingleServer() {
      RemoteCache<String, String> cache = SERVER_TEST
            .hotrod()
            .withCacheMode(CacheMode.DIST_SYNC).create();

      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
