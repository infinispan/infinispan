package org.infinispan.rest;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_20;
import static org.infinispan.rest.helper.RestServerHelper.STORE_PASSWORD;
import static org.infinispan.rest.helper.RestServerHelper.STORE_TYPE;
import static org.infinispan.util.concurrent.CompletionStages.join;

import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.ssl.SslContextName;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.netty.util.CharsetUtil;

/**
 * Most of the REST Server functionality is tested elsewhere. We can do that since
 * most of the implementation is exactly the same for both HTTP/1.1 and HTTP/2.0. Here we just do some basic sanity tests.
 *
 * @author Sebastian Łaskawiec
 */
@Test(groups = "functional", testName = "rest.Http2Test")
public final class Http2Test extends AbstractInfinispanTest {

   private static final String KEY_STORE_PATH = Http2Test.class.getClassLoader().getResource("./client.p12").getPath();

   private RestClient client;
   private RestServerHelper restServer;

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      if (restServer != null) {
         restServer.stop();
      }
      Util.close(client);
   }

   @DataProvider(name = "ssl-provider")
   public Object[][] opensslItemProvider() {
      return SslContextName.PROVIDER;
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldUseHTTP1WithALPN(String sslProvider) {
      secureUpgradeTest(HTTP_11, sslProvider);
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldUseHTTP2WithALPN(String sslProvider) {
      secureUpgradeTest(Protocol.HTTP_20, sslProvider);
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldUseHTTP2WithUpgrade(String sslProvider) {
      clearTextUpgrade(false, sslProvider);
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldUseHTTP2WithPriorKnowledge(String sslProvider) {
      clearTextUpgrade(true, sslProvider);
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldReportErrorCorrectly(String sslProvider) {
      restServer = RestServerHelper.defaultRestServer()
            .withKeyStore(KEY_STORE_PATH, STORE_PASSWORD, STORE_TYPE)
            .withTrustStore(KEY_STORE_PATH, STORE_PASSWORD, STORE_TYPE)
            .withSslProvider(sslProvider)
            .start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder config = new RestClientConfigurationBuilder();

      config.addServer().host(restServer.getHost()).port(restServer.getPort())
            .protocol(HTTP_20).priorKnowledge(true)
            .security().ssl().enable()
            .trustStoreFileName(KEY_STORE_PATH).trustStorePassword(STORE_PASSWORD).trustStoreType(STORE_TYPE)
            .keyStoreFileName(KEY_STORE_PATH).keyStorePassword(STORE_PASSWORD).keyStoreType(STORE_TYPE)
            .hostnameVerifier((hostname, session) -> true);

      client = RestClient.forConfiguration(config.build());

      CompletionStage<RestResponse> response = client.raw().get("/invalid");
      ResponseAssertion.assertThat(response).isNotFound();
   }

   @Test(dataProvider = "ssl-provider")
   public void shouldUseHTTP1(String sslProvider) {
      restServer = RestServerHelper.defaultRestServer()
            .withSslProvider(sslProvider)
            .start(TestResourceTracker.getCurrentTestShortName());
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort()).protocol(HTTP_11);
      client = RestClient.forConfiguration(builder.build());

      CompletionStage<RestResponse> response = client.cacheManager("default").info();
      ResponseAssertion.assertThat(response).isOk();

      RestEntity value = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "test".getBytes(CharsetUtil.UTF_8));
      response = client.cache("defaultcache").put("test", value);
      Assertions.assertThat(join(response).getStatus()).isEqualTo(204);

      Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);
   }

   private void clearTextUpgrade(boolean previousKnowledge, String sslProvider) {
      restServer = RestServerHelper.defaultRestServer()
            .withSslProvider(sslProvider)
            .start(TestResourceTracker.getCurrentTestShortName());
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort())
            .priorKnowledge(previousKnowledge).protocol(Protocol.HTTP_20);

      client = RestClient.forConfiguration(builder.build());

      CompletionStage<RestResponse> response = client.cacheManager("default").info();
      ResponseAssertion.assertThat(response).isOk();

      RestEntity value = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "test".getBytes(CharsetUtil.UTF_8));
      response = client.cache("defaultcache").post("test", value);

      Assertions.assertThat(join(response).getStatus()).isEqualTo(204);
      Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);
   }

   private void secureUpgradeTest(Protocol choice, String sslProvider) {
      //given
      restServer = RestServerHelper.defaultRestServer()
            .withKeyStore(KEY_STORE_PATH, STORE_PASSWORD, STORE_TYPE)
            .withSslProvider(sslProvider)
            .start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort()).protocol(choice)
            .security().ssl().trustStoreFileName(KEY_STORE_PATH).trustStorePassword(STORE_PASSWORD)
            .hostnameVerifier((hostname, session) -> true);

      client = RestClient.forConfiguration(builder.build());

      RestEntity value = RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, "test".getBytes(CharsetUtil.UTF_8));
      CompletionStage<RestResponse> response = client.cache("defaultcache").post("test", value);

      //then
      ResponseAssertion.assertThat(response).isOk();
      Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);
   }
}
