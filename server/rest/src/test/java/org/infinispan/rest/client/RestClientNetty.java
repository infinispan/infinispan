package org.infinispan.rest.client;

import static io.netty.buffer.Unpooled.wrappedBuffer;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfiguration;

import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
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
   private final HttpVersion version;
   private final String baseCacheURL;
   private final String baseServerURL;

   public RestClientNetty(RestClientConfiguration configuration) {
      this.configuration = configuration;
      httpClient = NettyHttpClient.forConfiguration(configuration);
      baseCacheURL = String.format("%s/v2/caches", configuration.contextPath());
      baseServerURL = String.format("%s/v2/server", configuration.contextPath());
      version = configuration.protocol() == Protocol.HTTP_11 ? HttpVersion.HTTP_1_1 : new HttpVersion("HTTP/2.0", true);
   }

   @Override
   public void close() {
      httpClient.stop();
   }

   private String buildURL(String cache, String key) {
      return baseCacheURL + "/" + sanitize(cache) + "/" + sanitize(key);
   }

   private String sanitize(String s) {
      try {
         return URLEncoder.encode(s, "UTF-8");
      } catch (UnsupportedEncodingException e) {
         return null;
      }
   }

   @Override
   public CompletionStage<RestResponse> cachePost(String cache, String key, String value) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.POST, buildURL(cache, key), wrappedBuffer(value.getBytes(CharsetUtil.UTF_8)));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> post(String url, Map<String, String> headers, Map<String, String> formParameters) {
      StringBuilder body = new StringBuilder();
      formParameters.forEach((k, v) -> {
         if (body.length() != 0) {
            body.append('&');
         }
         body.append(sanitize(k));
         body.append('=');
         body.append(sanitize(v));
      });
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.POST, url, wrappedBuffer(body.toString().getBytes(CharsetUtil.UTF_8)));
      headers.forEach((k, v) -> request.headers().set(k, v));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> cachePut(String cache, String key, String value) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.PUT, buildURL(cache, key), wrappedBuffer(value.getBytes(CharsetUtil.UTF_8)));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> cacheGet(String cache, String key) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.GET, buildURL(cache, key));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> cacheDelete(String cache, String key) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.DELETE, buildURL(cache, key));
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> cacheCreateFromTemplate(String cacheName, String template) {
      String url = String.format("%s/%s?template=%s", baseCacheURL, cacheName, template);
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.POST, url);
      return execute(request);
   }

   @Override
   public CompletionStage<RestResponse> serverConfig() {
      return serverGet("config");
   }

   @Override
   public CompletionStage<RestResponse> serverStop() {
      return serverGet("stop");
   }

   @Override
   public CompletionStage<RestResponse> serverThreads() {
      return serverGet("threads");
   }

   @Override
   public CompletionStage<RestResponse> serverMemory() {
      return serverGet("memory");
   }

   @Override
   public CompletionStage<RestResponse> serverEnv() {
      return serverGet("env");
   }

   @Override
   public CompletionStage<RestResponse> serverCacheManagers() {
      return serverGet("cache-managers");
   }

   @Override
   public CompletionStage<RestResponse> serverInfo() {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.GET, baseServerURL);
      return execute(request);
   }

   private CompletionStage<RestResponse> serverGet(String path) {
      DefaultFullHttpRequest request = new DefaultFullHttpRequest(version, HttpMethod.GET, baseServerURL + "/" + path);
      return execute(request);
   }

   private CompletionStage<RestResponse> execute(FullHttpRequest request) {
      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         switch (authentication.mechanism().toUpperCase()) {
            case "BASIC":
               String plain = authentication.username() + ":" + new String(authentication.password());
               String encoded = Base64.getEncoder().encodeToString(plain.getBytes());
               request.headers().set(HttpHeaderNames.AUTHORIZATION, "Basic " + encoded);
               break;
            case "DIGEST":
               break;
            case "BEARER":
               request.headers().set(HttpHeaderNames.AUTHORIZATION, "Bearer " + authentication.username());
               break;
            default:
               throw new IllegalArgumentException("cannot handle "+ authentication.mechanism() + " authentication");
         }
      }
      return httpClient.sendRequest(request).thenApply(RestResponseNetty::new);
   }
}
