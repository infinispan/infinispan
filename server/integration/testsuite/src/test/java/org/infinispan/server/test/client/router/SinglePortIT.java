package org.infinispan.server.test.client.router;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.rest.client.NettyHttpClient;
import org.infinispan.commons.junit.Cleanup;
import org.infinispan.server.test.category.Security;
import org.infinispan.server.test.util.security.SecurityConfigurationHelper;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;

/**
 * Tests Single Port feature.
 *
 * @author Sebastian Łaskawiec
 */
@RunWith(Arquillian.class)
@Category({Security.class})
@WithRunningServer({@RunningServer(name = "hotrodSslWithSinglePort", config = "testsuite/hotrod-ssl-with-single-port.xml")})
public class SinglePortIT {

   public static final String CACHE_NAME = "default";

   @Rule
   public Cleanup cleanup = new Cleanup();

   @Test
   public void testHttp2SwitchThroughUpgradeHeader() throws Exception {
      //given
      FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/" + CACHE_NAME + "/testHttp2SwitchThroughUpgradeHeader",
            wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host("localhost").port(8080).protocol(Protocol.HTTP_20);
      NettyHttpClient client = NettyHttpClient.forConfiguration(builder.build());
      try {
         //when
         FullHttpResponse response = client.sendRequest(putValueInCacheRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);

         //then
         assertEquals(HttpResponseStatus.OK, response.status());
      } finally {
         client.stop();
      }
   }

   @Test
   public void testHttp2SwitchThroughALPN() throws Exception {
      //given
      FullHttpRequest putValueInCacheRequest = new DefaultFullHttpRequest(HTTP_1_1, POST, "/rest/" + CACHE_NAME + "/testHttp2SwitchThroughALPN",
            wrappedBuffer("test".getBytes(CharsetUtil.UTF_8)));

      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.addServer().host("localhost").port(8443).protocol(Protocol.HTTP_20)
            .security().ssl().trustStoreFileName(SecurityConfigurationHelper.DEFAULT_TRUSTSTORE_PATH)
            .trustStorePassword(SecurityConfigurationHelper.DEFAULT_TRUSTSTORE_PASSWORD.toCharArray());
      NettyHttpClient client = NettyHttpClient.forConfiguration(builder.build());
      try {
         //when
         FullHttpResponse response = client.sendRequest(putValueInCacheRequest).toCompletableFuture().get(5, TimeUnit.SECONDS);

         //then
         assertEquals(HttpResponseStatus.OK, response.status());
      } finally {
         client.stop();
      }
   }

   @Test
   public void testHotRodSwitchThroughUpgradeHeader() {
      //given
      ConfigurationBuilder builder = new ConfigurationBuilder()
            .addServers("localhost:" + 8080);

      //when
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
      cleanup.add(remoteCacheManager);
      RemoteCache<String, String> cache = remoteCacheManager.getCache(CACHE_NAME);
      cache.put("testHotRodSwitchThroughUpgradeHeader", "test");

      //then
      assertEquals("test", cache.get("testHotRodSwitchThroughUpgradeHeader"));
   }

   @Test
   public void testHotRodSwitchThroughALPN() {
      //given
      ConfigurationBuilder builder = new SecurityConfigurationHelper()
            .withDefaultSsl()
            .addServers("localhost:" + 8443);

      //when
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
      cleanup.add(remoteCacheManager);
      RemoteCache<String, String> cache = remoteCacheManager.getCache(CACHE_NAME);
      cache.put("testHotRodSwitchThroughALPN", "test");

      //then
      assertEquals("test", cache.get("testHotRodSwitchThroughALPN"));
   }

}
