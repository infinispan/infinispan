package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/

@Category(Security.class)
@Tag("embedded")
public class AuthenticationMultiRealmIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationMultiRealm.xml")
               .addListener(new LdapServerListener())
               .build();

   public AuthenticationMultiRealmIT() {
   }

   @Test
   public void testLDAP() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().authentication()
            .saslMechanism("SCRAM-SHA-256")
            .serverName("infinispan")
            .realm("default")
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword())
            .ssl().sniHostName("infinispan.test");
      performOperations(builder);
   }

   @Test
   public void testProps() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security()
            .ssl().sniHostName("infinispan.test")
            .authentication()
            .saslMechanism("SCRAM-SHA-256")
            .serverName("infinispan")
            .realm("default")
            .username("all_user")
            .password("all");
      performOperations(builder);
   }

   @Test
   public void testCert() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(1).connectionPool().maxActive(1);
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      SERVERS.getServerDriver().applyKeyStore(builder, "admin.pfx");
      builder.security()
            .ssl().sniHostName("infinispan.test")
            .authentication()
            .saslMechanism("EXTERNAL")
            .serverName("infinispan")
            .realm("default");
      performOperations(builder);
   }

   @Test
   public void testUnknown() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security()
            .ssl().sniHostName("infinispan.test")
            .authentication()
            .saslMechanism("SCRAM-SHA-256")
            .serverName("infinispan")
            .realm("default")
            .username("unknown")
            .password("unknown");
      Exceptions.expectException(".*ELY05161.*",  () -> performOperations(builder), TransportException.class);
   }

   private void performOperations(ConfigurationBuilder builder) {
      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }
}
