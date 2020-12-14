package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.util.Version;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Ryan Emerson
 * @since 11.0
 */
public class AnchoredKeysIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AnchoredKeys.xml")
               .numServers(2)
               .runMode(ServerRunMode.EMBEDDED)
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testAnchoredKeysCache() {
      RemoteCacheManager rcm = SERVER_TEST.hotrod().createRemoteCacheManager();
      test(rcm.getCache("default"));
   }

   @Test
   public void testCreateAnchoredKeysCache() {
      BasicConfiguration config = new XMLStringConfiguration("<infinispan><cache-container><replicated-cache name=\"anchored2\">\n" +
                        "<locking concurrency-level=\"100\" acquire-timeout=\"1000\"/>\n" +
                           "<anchored-keys xmlns=\"urn:infinispan:config:anchored-keys:" + Version.getMajorMinor() + "\" enabled=\"true\"/>\n" +
                       "</replicated-cache></cache-container></infinispan>");
      RemoteCacheManager rcm = SERVER_TEST.hotrod().createRemoteCacheManager();
      rcm.administration().createCache("anchored2", config);
      test(rcm.getCache("anchored2"));
   }

   private void test(RemoteCache<String, String> cache) {
      assertNotNull(cache);
      cache.put("k1", "v1");
      assertEquals("v1", cache.get("k1"));
      assertEquals(1, cache.size());
   }
}
