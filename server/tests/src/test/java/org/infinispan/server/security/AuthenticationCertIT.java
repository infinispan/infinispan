package org.infinispan.server.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyContains;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Category(Security.class)
@Tag("embedded")
public class AuthenticationCertIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/AuthenticationServerTrustTest.xml")
               .build();

   @Test
   public void testTrustedCertificateHotRod() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(1).connectionPool().maxActive(1);
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      SERVERS.getServerDriver().applyKeyStore(builder, "admin.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      builder.security()
            .authentication()
            .saslMechanism("EXTERNAL")
            .serverName("infinispan")
            .realm("default");

      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testUntrustedCertificateHotRod() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(1).connectionPool().maxActive(1);
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      SERVERS.getServerDriver().applyKeyStore(builder, "untrusted.pfx");
      builder.security().ssl().sniHostName("infinispan.test");
      builder.security()
            .authentication()
            .saslMechanism("EXTERNAL")
            .serverName("infinispan")
            .realm("default");

      Exceptions.expectException(TransportException.class, () -> SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create());
   }

   @Test
   public void testTrustedCertificateREST_HTTP11() {
      testTrustedCertificateREST(Protocol.HTTP_11);
   }

   @Test
   public void testTrustedCertificateREST_HTTP20() {
      testTrustedCertificateREST(Protocol.HTTP_20);
   }

   private void testTrustedCertificateREST(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      SERVERS.getServerDriver().applyKeyStore(builder, "admin.pfx");
      builder
            .protocol(protocol)
            .security()
            .authentication()
            .ssl()
            .sniHostName("infinispan")
            .hostnameVerifier((hostname, session) -> true).connectionTimeout(120_000).socketTimeout(120_000);
      RestCacheClient cache = SERVERS.rest().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create().cache(SERVERS.getMethodName());

      assertStatus(NO_CONTENT, cache.put("k1", "v1"));
      assertStatusAndBodyContains(OK, "1", cache.size());
      assertStatusAndBodyContains(OK, "v1", cache.get("k1"));
   }

   @Test
   public void overviewReport() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      SERVERS.getServerDriver().applyTrustStore(builder, "ca.pfx");
      SERVERS.getServerDriver().applyKeyStore(builder, "admin.pfx");
      builder
            .protocol(Protocol.HTTP_20)
            .security()
            .authentication()
            .ssl()
            .sniHostName("infinispan")
            .hostnameVerifier((hostname, session) -> true).connectionTimeout(120_000).socketTimeout(120_000);

      try (RestClient restClient = SERVERS.rest().withClientConfiguration(builder)
            .withCacheMode(CacheMode.DIST_SYNC).create()) {
         CompletionStage<RestResponse> response = restClient.server().overviewReport();
         ResponseAssertion.assertThat(response).isOk();
         Json report = Json.read(join(response).getBody());
         Json security = report.at("security");
         assertThat(security.at("security-realms").at("default").at("tls").asString()).isEqualTo("CLIENT");
         assertThat(security.at("tls-endpoints").asJsonList()).extracting(Json::asString)
               .containsExactly("endpoint-default-default");
      } catch (Exception e) {
         fail(e);
      }
   }
}
