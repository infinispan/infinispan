package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Ryan Emerson
 * @since 11.0
 */
public class AnchoredKeysIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AnchoredKeys.xml")
               .numServers(2)
               .featuresEnabled("anchored-keys")
               .build();

   @Test
   public void testAnchoredKeysCache() {
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      test(rcm.getCache("default"));
   }

   @Test
   public void testCreateAnchoredKeysCache() {
      BasicConfiguration config = new StringConfiguration("""
         <replicated-cache name="anchored2">
         <locking concurrency-level="100" acquire-timeout="1000"/>
         <anchored-keys xmlns="urn:infinispan:config:anchored-keys" enabled="true"/>
         </replicated-cache>""".trim());
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
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
