package org.infinispan.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletionStage;

import javax.net.ssl.SSLHandshakeException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@Category(Security.class)
@Tag("embedded")
public class TLSWithoutAuthenticationIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/TLSWithoutAuthenticationTest.xml")
               .build();

   @Test
   public void testReadWrite() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();

      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testDisabledProtocol() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().protocol("TLSv1.1").sniHostName("infinispan.test");
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
      builder.security().ssl().ciphers("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384").sniHostName("infinispan.test");
      expectException(TransportException.class, ClosedChannelException.class,
                      () -> SERVERS.hotrod().withClientConfiguration(builder)
                                       .withCacheMode(CacheMode.DIST_SYNC)
                                       .create());
   }

   @Test
   public void testForceTLSv12() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().protocol("TLSv1.2").sniHostName("infinispan.test");
      SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
   }

   @Test
   public void testForceTLSv13() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().protocol("TLSv1.3").sniHostName("infinispan.test");
      SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
   }

   @Test
   public void overviewReport() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      builder.security().ssl().sniHostName("infinispan.test").hostnameVerifier((hostname, session) -> true);

      try (RestClient restClient = SERVERS.rest().withClientConfiguration(builder)
            .withCacheMode(CacheMode.DIST_SYNC).create()) {
         CompletionStage<RestResponse> response = restClient.server().overviewReport();
         ResponseAssertion.assertThat(response).isOk();
         Json report = Json.read(join(response).getBody());
         Json security = report.at("security");
         assertThat(security.at("security-realms").at("default").at("tls").asString()).isEqualTo("SERVER");
         assertThat(security.at("tls-endpoints").asJsonList()).extracting(Json::asString)
               .containsExactly("endpoint-default-default");
      } catch (Exception e) {
         fail(e);
      }
   }
}
