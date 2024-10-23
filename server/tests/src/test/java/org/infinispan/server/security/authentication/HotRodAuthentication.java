package org.infinispan.server.security.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Security
public class HotRodAuthentication {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = AuthenticationIT.SERVERS;

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
}
