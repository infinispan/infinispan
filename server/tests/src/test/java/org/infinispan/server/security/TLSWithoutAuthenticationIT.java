package org.infinispan.server.security;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.channels.ClosedChannelException;

import javax.net.ssl.SSLHandshakeException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@Category(Security.class)
public class TLSWithoutAuthenticationIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/TLSWithoutAuthenticationTest.xml")
               .build();

   @Test
   public void testHotRodReadWrite() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testDisabledProtocol() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().protocol("TLSv1.1");
      try {
         SERVERS.hotrod().withClientConfiguration(builder)
                    .withCacheMode(CacheMode.DIST_SYNC)
                    .create();
      } catch (Throwable t) {
         assertEquals(TransportException.class, t.getClass());
         assertTrue(t.getCause() instanceof SSLHandshakeException || t.getCause() instanceof ClosedChannelException,
               "Unexpected exception: " + t.getCause());
      }
   }

   @Test
   public void testDisabledCipherSuite() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().ciphers("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
      expectException(TransportException.class, ClosedChannelException.class,
                      () -> SERVERS.hotrod().withClientConfiguration(builder)
                                       .withCacheMode(CacheMode.DIST_SYNC)
                                       .create());
   }

   @Test
   public void testForceTLSv12() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().protocol("TLSv1.2");
      SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
   }

   @Test
   public void testForceTLSv13() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().protocol("TLSv1.3");
      SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
   }
}
