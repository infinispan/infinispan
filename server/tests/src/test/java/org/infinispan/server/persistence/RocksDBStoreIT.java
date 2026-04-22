package org.infinispan.server.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.util.Version;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.tags.Persistence;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Persistence
public class RocksDBStoreIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/CustomStoreTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .mavenArtifacts("org.infinispan:infinispan-cachestore-rocksdb:" + Version.getVersion(),
                     "org.rocksdb:rocksdbjni:" + System.getProperty("version.rocksdb"))
               .build();

   @Test
   public void testRocksDBStore() {
      String cacheName = "rocksdb-cache";
      String config = String.format("""
            <distributed-cache name="%s">
               <persistence>
                  <rocksdb-store xmlns="urn:infinispan:config:store:rocksdb:%s"/>
               </persistence>
            </distributed-cache>""", cacheName, Version.getMajorMinor());
      RemoteCache<String, String> cache = SERVERS.hotrod().withServerConfiguration(new StringConfiguration(config)).create();

      cache.put("k1", "v1");
      cache.put("k2", "v2");
      cache.put("k3", "v3");
      assertEquals("v1", cache.get("k1"));
      assertEquals("v2", cache.get("k2"));
      assertEquals("v3", cache.get("k3"));
      assertEquals(3, cache.size());

      cache.put("k2", "v2-updated");
      assertEquals("v2-updated", cache.get("k2"));

      cache.remove("k1");
      assertNull(cache.get("k1"));
      assertEquals(2, cache.size());
   }
}
