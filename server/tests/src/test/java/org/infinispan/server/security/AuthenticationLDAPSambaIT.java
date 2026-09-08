package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.LdapServerListener;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.infinispan.testing.jupiter.tags.Security;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises the LDAP realm against a Samba 4 Active Directory Domain Controller,
 * started by {@link LdapServerListener} when {@code -Dorg.infinispan.test.ldapServer=samba}
 * is set. Active Directory stores only write-only password hashes, so the realm must
 * verify credentials by binding as the user (direct verification). That approach
 * supports the mechanisms that can be satisfied by a bind (PLAIN, DIGEST-SHA); SCRAM
 * is not supported because it requires server-side access to the password material.
 *
 * @since 16.3
 **/
@Security
public class AuthenticationLDAPSambaIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationLDAPSambaTest.xml")
               .property(TestSystemPropertyNames.LDAP_SERVER, "samba")
               .addListener(new LdapServerListener())
               .build();

   @ParameterizedTest
   @ValueSource(strings = {"PLAIN", "DIGEST-SHA-512", "DIGEST-SHA-384", "DIGEST-SHA-256"})
   public void testReadWrite(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism(mechanism)
            .serverName("infinispan")
            .realm("default")
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());

      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testBruteForceProtection() {
      String user = TestUser.DEPLOYER.getUser();
      // All attempts must target the same server node so that the failure counter reaches the threshold
      for (int i = 0; i < 10; i++) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.security().authentication()
               .saslMechanism("DIGEST-SHA-256")
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
            .saslMechanism("DIGEST-SHA-256")
            .serverName("infinispan")
            .realm("default")
            .username(user)
            .password(TestUser.DEPLOYER.getPassword());
       assertThrows(HotRodClientException.class, () ->
             SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create(0)
       );
    }

    @Test
    public void testEmptyPasswordRejected() {
       String user = TestUser.READER.getUser();
       ConfigurationBuilder validBuilder = new ConfigurationBuilder();
       validBuilder.security().authentication()
             .saslMechanism("PLAIN")
             .serverName("infinispan")
             .realm("default")
             .username(user)
             .password(TestUser.READER.getPassword());
       // Positive control: the same user and mechanism authenticate with the correct password
       RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(validBuilder).withCacheMode(CacheMode.DIST_SYNC).create();
       assertNull(cache.get("k"));

       ConfigurationBuilder emptyBuilder = new ConfigurationBuilder();
       emptyBuilder.security().authentication()
             .saslMechanism("PLAIN")
             .serverName("infinispan")
             .realm("default")
             .username(user)
             .password("");
       assertThrows(HotRodClientException.class, () ->
             SERVERS.hotrod().withClientConfiguration(emptyBuilder).withCacheMode(CacheMode.DIST_SYNC).create(0)
       );
    }
}
