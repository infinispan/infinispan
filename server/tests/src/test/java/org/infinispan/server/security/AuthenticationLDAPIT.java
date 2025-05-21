package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Category(Security.class)
@Tag("embedded")
public class AuthenticationLDAPIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationLDAPTest.xml")
                                    .addListener(new LdapServerListener())
                                    .build();

   @ParameterizedTest
   @ArgumentsSource(Common.SaslMechsArgumentProvider.class)
   public void testReadWrite(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (!mechanism.isEmpty()) {
         builder.security().authentication()
               .saslMechanism(mechanism)
               .serverName("infinispan")
               .realm("default")
               .username(TestUser.ADMIN.getUser())
               .password(TestUser.ADMIN.getPassword());
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
