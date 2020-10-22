package org.infinispan.server.security;

import static org.junit.Assert.assertEquals;

import javax.net.ssl.SSLHandshakeException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.commons.test.Exceptions;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Category(Security.class)
public class AuthenticationCertIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthenticationTrustTest.xml")
                                    .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testTrustedCertificate() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca");
      SERVERS.getServerDriver().applyKeyStore(builder, "admin");
      builder.security()
            .authentication()
            .saslMechanism("EXTERNAL")
            .serverName("infinispan")
            .realm("default");

      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testUntrustedCertificate() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca");
      SERVERS.getServerDriver().applyKeyStore(builder, "untrusted");
      builder.security()
            .authentication()
            .saslMechanism("EXTERNAL")
            .serverName("infinispan")
            .realm("default");

      Exceptions.expectException(TransportException.class, SSLHandshakeException.class, () -> SERVER_TEST.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create());
   }
}
