package org.infinispan.rest;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.rest.client.NettyHttpClient;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.ssl.OpenSsl;
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
   private static final String CACHE_MANAGER_PATH = "/rest/v2/cache-managers/DefaultCacheManager";

   private NettyHttpClient client;
   private RestServerHelper restServer;

   @AfterMethod(alwaysRun = true)
   public void afterMethod() {
      if (restServer != null) {
         restServer.stop();
      }
      if (client != null) {
         client.stop();
      }
   }

   @Test
   public void shouldUseHTTP1WithALPN() throws Exception {
      secureUpgradeTest(HTTP_11);
   }

   @Test
   public void shouldUseHTTP2WithALPN() throws Exception {
      secureUpgradeTest(Protocol.HTTP_20);
   }

   @Test
   public void shouldUseHTTP2WithUpgrade() throws Exception {
      clearTextUpgrade(false);
   }

   @Test
   public void shouldUseHTTP2WithPriorKnowledge() throws Exception {
      clearTextUpgrade(true);
   }

   @Test
   public void shouldUseHTTP1() throws Exception {
      restServer = RestServerHelper.defaultRestServer().start(TestResourceTracker.getCurrentTestShortName());
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort()).protocol(HTTP_11);
      client = NettyHttpClient.forConfiguration(builder.build());

      FullHttpRequest getRequest = new DefaultFullHttpRequest(HTTP_1_1, GET, CACHE_MANAGER_PATH);
      FullHttpResponse response = client.sendRequest(getRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);
      Assertions.assertThat(response.status().code()).isEqualTo(200);

      FullHttpRequest postRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, restServer.getBasePath() + "/test", wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));
      response = client.sendRequest(postRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);
      Assertions.assertThat(response.status().code()).isEqualTo(204);
      Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);
   }

   private void clearTextUpgrade(boolean previousKnowledge) throws Exception {
      restServer = RestServerHelper.defaultRestServer().start(TestResourceTracker.getCurrentTestShortName());
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort())
            .priorKnowledge(previousKnowledge).protocol(Protocol.HTTP_20);

      client = NettyHttpClient.forConfiguration(builder.build());

      FullHttpRequest getRequest = new DefaultFullHttpRequest(HTTP_1_1, GET, CACHE_MANAGER_PATH);
      FullHttpResponse response = client.sendRequest(getRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);
      Assertions.assertThat(response.status().code()).isEqualTo(200);

      FullHttpRequest postRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, restServer.getBasePath() + "/test", wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));
      response = client.sendRequest(postRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);
      Assertions.assertThat(response.status().code()).isEqualTo(204);
      Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);
   }

   private void secureUpgradeTest(Protocol choice) throws Exception {
      if (!OpenSsl.isAlpnSupported()) {
         throw new IllegalStateException("OpenSSL is not present, can not test TLS/ALPN support. " +
               "Version: " + OpenSsl.versionString() + " Cause: " + OpenSsl.unavailabilityCause());
      }

      //given
      restServer = RestServerHelper.defaultRestServer()
            .withKeyStore(KEY_STORE_PATH, "secret", "pkcs12")
            .start(TestResourceTracker.getCurrentTestShortName());

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host(restServer.getHost()).port(restServer.getPort()).protocol(choice)
            .security().ssl().trustStoreFileName(KEY_STORE_PATH).trustStorePassword("secret".toCharArray());

      client = NettyHttpClient.forConfiguration(builder.build());

      FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST,
            restServer.getBasePath() + "/test",
            wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

      //when
      FullHttpResponse response = client.sendRequest(putValueInCacheRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);

      //then
      Assertions.assertThat(response.status().code()).isEqualTo(204);
      Assertions.assertThat(restServer.getCacheManager().getCache().size()).isEqualTo(1);
   }
}
