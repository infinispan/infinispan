package org.infinispan.server.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

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
 * Exercises the LDAP realm against a 389 Directory Server, started by {@link LdapServerListener} when
 * {@code -Dorg.infinispan.test.ldapServer=threeeightnineds} is set.
 *
 * <p>A 389 Directory Server stores its userPassword hashes where privileged accounts can read them, so the realm binds as cn=Directory Manager and verifies credentials against the stored hash (shadow verification), like {@link AuthenticationLDAPIT}. The official container image re-hashes every written value with PBKDF2-SHA512 and never retains password material in clear text, therefore DIGEST mechanisms are not supported by this backend; PLAIN and SCRAM can be satisfied from the derived key.
 * <p>The image's default of 100,000 PBKDF2 iterations exceeds Elytron's default maximum for SCRAM (wildfly.sasl.scram.max-iteration-count = 32768) on both the server and the client, so the endpoint configuration and the client of this test raise that property to allow SCRAM verification against this backend.
 *
 * @since 16.4
 **/
@Security
public class AuthenticationLDAPThreeEightNineDSIT {
   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationLDAPThreeEightNineDSTest.xml")
               .property(TestSystemPropertyNames.LDAP_SERVER, "threeeightnineds")
               .addListener(new LdapServerListener())
               .build();

   @ParameterizedTest
   @ValueSource(strings = {"PLAIN", "SCRAM-SHA-512"})
   public void testReadWrite(String mechanism) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism(mechanism)
            .serverName("infinispan")
            .realm("default")
            .username(TestUser.ADMIN.getUser())
            .password(TestUser.ADMIN.getPassword());
      if (mechanism.startsWith("SCRAM")) {
         // The 389 Directory Server image re-hashes passwords with 100,000 PBKDF2 iterations, which exceeds
         // Elytron's client-side SCRAM default maximum (wildfly.sasl.scram.max-iteration-count = 32768)
         builder.security().authentication().saslProperties(
               Map.of("wildfly.sasl.scram.max-iteration-count", "262144"));
      }

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
               .saslMechanism("PLAIN")
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
            .saslMechanism("PLAIN")
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
