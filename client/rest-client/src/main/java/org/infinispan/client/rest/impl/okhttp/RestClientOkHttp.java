package org.infinispan.client.rest.impl.okhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.ServerConfiguration;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestClientOkHttp implements RestClient {
   private static final MediaType TEXT_PLAIN = MediaType.parse("text/plain; charset=utf-8");
   private final RestClientConfiguration configuration;
   private final OkHttpClient httpClient;
   private final String baseURL;
   private final String baseCacheURL;
   private final String baseServerURL;

   public RestClientOkHttp(RestClientConfiguration configuration) {
      this.configuration = configuration;
      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder
            .connectTimeout(configuration.connectionTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(configuration.socketTimeout(), TimeUnit.MILLISECONDS);

      SSLContext sslContext = configuration.security().ssl().sslContext();
      if (sslContext != null) {
         builder.sslSocketFactory(sslContext.getSocketFactory());
      }

      switch (configuration.protocol()) {
         case HTTP_11:
            builder.protocols(Arrays.asList(Protocol.HTTP_1_1));
            break;
         case HTTP_20:
            if (sslContext == null) {
               builder.protocols(Arrays.asList(Protocol.H2_PRIOR_KNOWLEDGE));
            } else {
               builder.protocols(Arrays.asList(Protocol.HTTP_2, Protocol.HTTP_1_1));
            }
            break;
      }

      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         switch (authentication.mechanism()) {
            case "BASIC":
               builder.authenticator(new BasicAuthenticator(authentication));
               break;
            case "BEARER_TOKEN":
               builder.authenticator(new BearerAuthenticator(authentication));
               break;
            case "DIGEST":
               builder.authenticator(new DigestAuthenticator(authentication));
               break;
            default:
               throw new IllegalArgumentException("Cannot handle " + authentication.mechanism());
         }
      }

      httpClient = builder.build();
      ServerConfiguration server = configuration.servers().get(0);
      baseURL = String.format("%s://%s:%d", sslContext == null ? "http" : "https", server.host(), server.port());
      baseCacheURL = String.format("%s%s/v2/caches", baseURL, configuration.contextPath()).replaceAll("//", "/");
      baseServerURL = String.format("%s%s/v2/server", baseURL, configuration.contextPath()).replaceAll("//", "/");
   }

   @Override
   public void close() throws IOException {
      httpClient.dispatcher().executorService().shutdownNow();
      httpClient.connectionPool().evictAll();
      if (httpClient.cache() != null) {
         httpClient.cache().close();
      }
   }

   private String cacheUrl(String cache, String key) {
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
   public CompletionStage<RestResponse> post(String url, Map<String, String> headers, Map<String, String> formParameters) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseURL + url);
      headers.forEach((k, v) -> builder.header(k, v));
      FormBody.Builder form = new FormBody.Builder();
      formParameters.forEach((k, v) -> form.add(k, v));
      builder.post(form.build());
      return execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> cachePost(String cache, String key, String value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl(cache, key)).post(RequestBody.create(TEXT_PLAIN, value));
      return execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> cachePut(String cache, String key, String value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl(cache, key)).put(RequestBody.create(TEXT_PLAIN, value));
      return execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> cacheGet(String cache, String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl(cache, key)).get();
      return execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> cacheDelete(String cache, String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl(cache, key)).delete();
      return execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> cacheCreateFromTemplate(String cacheName, String template) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/%s?template=%s", baseCacheURL, sanitize(cacheName), template)).post(RequestBody.create(TEXT_PLAIN, ""));
      return execute(builder);
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
      Request.Builder builder = new Request.Builder();
      builder.url(baseServerURL);
      return execute(builder);
   }

   private CompletionStage<RestResponse> serverGet(String path) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseServerURL + "/" + path);
      return execute(builder);
   }

   private CompletionStage<RestResponse> execute(Request.Builder request) {
      CompletableFuture<RestResponse> response = new CompletableFuture<>();
      httpClient.newCall(request.build()).enqueue(new Callback() {
         @Override
         public void onFailure(Call call, IOException e) {
            response.completeExceptionally(e);
         }

         @Override
         public void onResponse(Call call, Response r) throws IOException {
            response.complete(new RestResponseOkHttp(r));
         }
      });
      return response;
   }

}
