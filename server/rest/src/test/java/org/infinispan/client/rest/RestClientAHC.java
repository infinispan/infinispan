package org.infinispan.client.rest;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Realm;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.filter.FilterContext;
import org.asynchttpclient.filter.RequestFilter;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.ServerConfiguration;

import io.netty.handler.ssl.SslContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestClientAHC implements RestClient {
   private final RestClientConfiguration configuration;
   private final AsyncHttpClient httpClient;
   private final String baseURL, baseServerURL, baseCachesURL;

   public RestClientAHC(RestClientConfiguration configuration) {
      this.configuration = configuration;
      SslContext sslContext = NettyTruststoreUtil.createSslContext(configuration);
      DefaultAsyncHttpClientConfig.Builder builder = new DefaultAsyncHttpClientConfig.Builder();
      builder
            .setConnectTimeout(configuration.connectionTimeout())
            .setTcpNoDelay(configuration.tcpNoDelay())
            .setKeepAlive(configuration.tcpKeepAlive())
            .setReadTimeout(configuration.socketTimeout())
            .setSslContext(sslContext);
      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         if ("Bearer".equalsIgnoreCase(authentication.mechanism())) {
            builder.addRequestFilter(new AddHeaderRequestFilter("Authorization", "Bearer " + authentication.username()));
         } else {
            Realm.Builder realmBuilder = new Realm.Builder(authentication.username(), new String(authentication.password()));

            realmBuilder
                  .setScheme(Realm.AuthScheme.valueOf(authentication.mechanism()))
                  .setRealmName(authentication.realm());
            builder.setRealm(realmBuilder.build());
         }
      }
      httpClient = new DefaultAsyncHttpClient(builder.build());
      ServerConfiguration server = configuration.servers().get(0);
      baseURL = String.format("%s://%s:%d%s/v2/", sslContext == null ? "http" : "https", server.host(), server.port(), configuration.contextPath());
      baseCachesURL = baseURL + "caches";
      baseServerURL = baseURL + "server";
   }

   @Override
   public void close() throws IOException {
      httpClient.close();
   }

   private String buildURL(String cache, String key) {
      return baseCachesURL + "/" + cache + "/" + key;
   }

   public CompletionStage<RestResponse> post(String cache, String key, String value) {
      BoundRequestBuilder request = httpClient.preparePost(buildURL(cache, key));
      request.setBody(value);
      return execute(request);
   }

   public CompletionStage<RestResponse> put(String cache, String key, String value) {
      BoundRequestBuilder request = httpClient.preparePut(buildURL(cache, key));
      request.setBody(value);
      return execute(request);
   }

   public CompletionStage<RestResponse> get(String cache, String key) {
      BoundRequestBuilder request = httpClient.prepareGet(buildURL(cache, key));
      return execute(request);
   }

   public CompletionStage<RestResponse> delete(String cache, String key) {
      BoundRequestBuilder request = httpClient.prepareDelete(buildURL(cache, key));
      return execute(request);
   }

   public CompletionStage<RestResponse> createCacheFromTemplate(String cacheName, String template) {
      String url = String.format("%s/%s?template=%s", baseCachesURL, cacheName, template);
      BoundRequestBuilder request = httpClient.preparePost(url);
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
   public CompletionStage<RestResponse> serverInfo() {
      BoundRequestBuilder request = httpClient.prepareGet(baseServerURL);
      return execute(request);
   }

   private CompletionStage<RestResponse> serverGet(String path) {
      BoundRequestBuilder request = httpClient.prepareGet(baseServerURL + "/" + path);
      return execute(request);
   }

   private CompletionStage<RestResponse> execute(BoundRequestBuilder request) {
      return request.execute().toCompletableFuture().thenApply(RestResponseAHC::new);
   }

   static class AddHeaderRequestFilter implements RequestFilter {
      final String name;
      final String value;

      public AddHeaderRequestFilter(String name, String value) {
         this.name = name;
         this.value = value;
      }

      @Override
      public <T> FilterContext<T> filter(FilterContext<T> ctx) {
         Request request = ctx.getRequest();
         RequestBuilder requestBuilder = new RequestBuilder(request);
         requestBuilder.addHeader(name, value);
         return new FilterContext.FilterContextBuilder<>(ctx).request(requestBuilder.build()).build();
      }
   }
}
