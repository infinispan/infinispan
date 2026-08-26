package org.infinispan.server.security.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.infinispan.testing.jupiter.tags.Security;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Security
public class HotRodAuthentication {

   @InfinispanServer(AuthenticationIT.class)
   public static TestClientDriver SERVERS;

   @ParameterizedTest
   @ArgumentsSource(Common.SaslMechsArgumentProvider.class)
   public void testHotRodReadWrite(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder.security().authentication()
               .saslMechanism(mechanism)
               .serverName("infinispan")
               .realm("default")
               .username("all_user")
               .password("all");
      }

      try {
         RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k1", "v1");
         assertEquals(1, cache.size());
         assertEquals("v1", cache.get("k1"));
      } catch (HotRodClientException e) {
         // Rethrow if unexpected
         if (!mechanism.isEmpty()) throw e;
      }
   }

   @Test
   public void testBruteForceProtection() {
      String user = TestUser.DEPLOYER.getUser();
      // All attempts must target the same server node so that the failure counter reaches the threshold
      for (int i = 0; i < 10; i++) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.security().authentication()
               .saslMechanism("SCRAM-SHA-256")
               .serverName("infinispan")
               .realm("default")
               .username(user)
               .password("wrongPassword");
         assertThrows(HotRodClientException.class, () ->
               SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create(0)
         );
      }
      // After exceeding max failed attempts, correct credentials should also be rejected
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism("SCRAM-SHA-256")
            .serverName("infinispan")
            .realm("default")
            .username(user)
            .password(TestUser.DEPLOYER.getPassword());
      assertThrows(HotRodClientException.class, () ->
            SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create(0)
      );
   }
}
