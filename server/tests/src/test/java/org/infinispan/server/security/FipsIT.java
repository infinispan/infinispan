package org.infinispan.server.security;

import static org.infinispan.testing.Exceptions.expectException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.net.ssl.SSLHandshakeException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.ResponseAssertion;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Security
public class FipsIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/FipsTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_FIPS_MODE, "true")
               .build();

   @Test
   public void testHotRodReadWrite() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withClientConfiguration(builder)
            .withCacheMode(CacheMode.DIST_SYNC)
            .create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testRestReadWrite() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      RestClient restClient = SERVERS.rest()
            .withClientConfiguration(builder)
            .create();
      ResponseAssertion.assertThat(restClient.server().info()).isOk();
   }

   @Test
   public void testFipsRejectsNonCompliantCipher() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl()
            .ciphers("TLS_CHACHA20_POLY1305_SHA256")
            .sniHostName("infinispan.test");
      expectException(TransportException.class, SSLHandshakeException.class,
            () -> SERVERS.hotrod()
                  .withClientConfiguration(builder)
                  .withCacheMode(CacheMode.DIST_SYNC)
                  .create());
   }
}
