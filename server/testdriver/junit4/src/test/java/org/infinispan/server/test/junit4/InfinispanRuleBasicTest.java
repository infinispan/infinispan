package org.infinispan.server.test.junit4;

import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.ProtocolVersion;
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
            // TODO this test uses the latest public image container version
            //  see ContainerInfinispanServerDriver
            //  At this very moment we don't have any supporting the #PROTOCOL_VERSION_40
            //  we can remove this downgrade, when the new version of Infinispan container
            //  (supporting the protocol 40) is published!
            .hotrod(builder -> builder.version(ProtocolVersion.PROTOCOL_VERSION_31))
            .withCacheMode(CacheMode.DIST_SYNC).create();

      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
