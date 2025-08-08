package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests transport TLS
 *
 * @author Pedro Ruivo
 * @since 14.0
 **/
@Category(Security.class)
@Tag("embedded")
public class TransportTLSIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/TransportTLSTest.xml")
               .numServers(2)
               .build();

   @Test
   public void testReadWrite() {
      ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
      hotRodBuilder.security().authentication()
            .serverName("infinispan")
            .realm("default")
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withClientConfiguration(hotRodBuilder)
            .withCacheMode(CacheMode.DIST_SYNC)
            .create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
