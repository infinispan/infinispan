package org.infinispan.client.rest;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.NettyHttpClient;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfiguration;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestClientNetty implements RestClient {
   private final RestClientConfiguration configuration;
   private final NettyHttpClient httpClient;
   private final String baseURL;
   private final HttpVersion version;

   public RestClientNetty(RestClientConfiguration configuration) {
      this.configuration = configuration;
      httpClient = NettyHttpClient.forConfiguration(configuration);
      baseURL = String.format("%s/v2/caches", configuration.contextPath());
      version = configuration.protocol() == Protocol.HTTP_11 ? HttpVersion.HTTP_1_1 : new HttpVersion("HTTP/2", true);
   }

   @Override
   public void close() throws IOException {
      httpClient.stop();
   }

   private String buildURL(String cache, String key) {
      return baseURL + "/" + cache + "/" + key;
   }

   @Override
   public CompletionStage<RestResponse> post(String cache, String key, String value) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.POST, buildURL(cache, key), wrappedBuffer(value.getBytes(CharsetUtil.UTF_8)));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> put(String cache, String key, String value) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.PUT, buildURL(cache, key), wrappedBuffer(value.getBytes(CharsetUtil.UTF_8)));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> get(String cache, String key) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.GET, buildURL(cache, key));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> delete(String cache, String key) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.DELETE, buildURL(cache, key));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> createCacheFromTemplate(String cacheName, String template) {
      String url = String.format("%s/%s?template=%s", baseURL, cacheName, template);
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.POST, url);
      return execute(request);
   }

   private CompletionStage<RestResponse> execute(FullHttpRequest request) {
      return httpClient.sendRequest(request).thenApply(RestResponseNetty::new);
   }
}
