package org.infinispan.client.rest.impl.okhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.client.rest.RestSecurityClient;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.client.rest.RestTaskClient;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.ServerConfiguration;
import org.infinispan.client.rest.configuration.SslConfiguration;
import org.infinispan.client.rest.impl.okhttp.auth.AbstractAuthenticator;
import org.infinispan.client.rest.impl.okhttp.auth.AutoDetectAuthenticator;
import org.infinispan.client.rest.impl.okhttp.auth.BasicAuthenticator;
import org.infinispan.client.rest.impl.okhttp.auth.BearerAuthenticator;
import org.infinispan.client.rest.impl.okhttp.auth.CachingAuthenticator;
import org.infinispan.client.rest.impl.okhttp.auth.CachingAuthenticatorInterceptor;
import org.infinispan.client.rest.impl.okhttp.auth.CachingAuthenticatorWrapper;
import org.infinispan.client.rest.impl.okhttp.auth.DigestAuthenticator;
import org.infinispan.client.rest.impl.okhttp.auth.NegotiateAuthenticator;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.SslContextFactory;

import okhttp3.Cache;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestClientOkHttp implements RestClient {
   static final Log log = LogFactory.getLog(RestClientOkHttp.class);
   static final MediaType TEXT_PLAIN = MediaType.parse("text/plain; charset=utf-8");
   static final RequestBody EMPTY_BODY = RequestBody.create(null, new byte[0]);
   private final RestClientConfiguration configuration;
   private final OkHttpClient httpClient;
   private final String baseURL;

   public RestClientOkHttp(RestClientConfiguration configuration) {
      this.configuration = configuration;
      OkHttpClient.Builder builder = new OkHttpClient.Builder();
      builder
            .connectTimeout(configuration.connectionTimeout(), TimeUnit.MILLISECONDS)
            .readTimeout(configuration.socketTimeout(), TimeUnit.MILLISECONDS).followRedirects(configuration.followRedirects());
      SslConfiguration ssl = configuration.security().ssl();
      if (ssl.enabled()) {
         SSLContext sslContext = ssl.sslContext();
         if (sslContext == null) {
            SslContextFactory sslContextFactory = new SslContextFactory()
                  .keyStoreFileName(ssl.keyStoreFileName())
                  .keyStorePassword(ssl.keyStorePassword())
                  .keyStoreType(ssl.keyStoreType())
                  .trustStoreFileName(ssl.trustStoreFileName())
                  .trustStorePassword(ssl.trustStorePassword())
                  .trustStoreType(ssl.trustStoreType())
                  .sslProtocol(ssl.protocol())
                  .provider(ssl.provider())
                  .classLoader(Thread.currentThread().getContextClassLoader())
                  .useNativeIfAvailable(false);
            sslContext = sslContextFactory.getContext();
            try {
               TrustManagerFactory tmf = sslContextFactory.getTrustManagerFactory();
               builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) tmf.getTrustManagers()[0]);
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } else {
            builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) ssl.trustManagers()[0]);
         }
         if (ssl.hostnameVerifier() != null) {
            builder.hostnameVerifier(ssl.hostnameVerifier());
         }
      }

      switch (configuration.protocol()) {
         case HTTP_11:
            builder.protocols(Collections.singletonList(Protocol.HTTP_1_1));
            break;
         case HTTP_20:
            if (!ssl.enabled()) {
               builder.protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE));
            } else {
               builder.protocols(Arrays.asList(Protocol.HTTP_2, Protocol.HTTP_1_1));
            }
            break;
      }

      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
         CachingAuthenticator authenticator;
         switch (authentication.mechanism()) {
            case "AUTO":
               authenticator = new AutoDetectAuthenticator(authentication);
               break;
            case "BASIC":
               authenticator = new BasicAuthenticator(authentication);
               break;
            case "BEARER_TOKEN":
               authenticator = new BearerAuthenticator(authentication);
               break;
            case "DIGEST":
               authenticator = new DigestAuthenticator(authentication);
               break;
            case "SPNEGO":
               authenticator = new NegotiateAuthenticator(authentication);
               break;
            default:
               throw new IllegalArgumentException("Cannot handle " + authentication.mechanism());
         }
         builder.addInterceptor(new CachingAuthenticatorInterceptor(authCache));
         builder.authenticator(new CachingAuthenticatorWrapper(authenticator, authCache));
      }

      httpClient = builder.build();
      ServerConfiguration server = configuration.servers().get(0);
      baseURL = String.format("%s://%s:%d", ssl.enabled() ? "https" : "http", server.host(), server.port()).replaceAll("//", "/");
   }

   @Override
   public RestClientConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public void close() throws IOException {
      httpClient.dispatcher().executorService().shutdownNow();
      ConnectionPool connectionPool = httpClient.connectionPool();
      connectionPool.evictAll();

      Cache cache = httpClient.cache();
      if (cache != null) {
         cache.close();
      }
   }

   @Override
   public CompletionStage<RestResponse> cacheManagers() {
      return execute(baseURL, configuration.contextPath(), "v2", "server", "cache-managers");
   }

   @Override
   public RestCacheManagerClient cacheManager(String name) {
      return new RestCacheManagerClientOkHttp(this, name);
   }

   @Override
   public RestContainerClient container() {
      return new RestContainerClientOkHttp(this);
   }

   @Override
   public CompletionStage<RestResponse> caches() {
      return execute(baseURL, configuration.contextPath(), "v2", "caches");
   }

   @Override
   public RestClusterClient cluster() {
      return new RestClusterClientOkHttp(this);
   }

   @Override
   public RestServerClient server() {
      return new RestServerClientOkHttp(this);
   }

   @Override
   public RestCacheClient cache(String name) {
      return new RestCacheClientOkHttp(this, name);
   }

   @Override
   public CompletionStage<RestResponse> counters() {
      return execute(baseURL, configuration.contextPath(), "v2", "counters");
   }

   @Override
   public RestCounterClient counter(String name) {
      return new RestCounterClientOkHttp(this, name);
   }

   @Override
   public RestTaskClient tasks() {
      return new RestTaskClientOkHttp(this);
   }

   @Override
   public RestRawClient raw() {
      return new RestRawClientOkHttp(this);
   }

   static String sanitize(String s) {
      try {
         return URLEncoder.encode(s, "UTF-8");
      } catch (UnsupportedEncodingException e) {
         // never going to happen
         return null;
      }
   }

   static Request.Builder addEnumHeader(String name, Request.Builder builder, Enum[] es) {
      if (es != null && es.length > 0) {
         StringJoiner joined = new StringJoiner(" ");
         for (Enum e : es) {
            joined.add(e.name());
         }
         builder.header(name, joined.toString());
      }
      return builder;
   }

   @Override
   public RestMetricsClient metrics() {
      return new RestMetricsClientOkHttp(this);
   }

   @Override
   public RestSchemaClient schemas() {
      return new RestSchemasClientOkHttp(this);
   }

   @Override
   public RestSecurityClient security() {
      return new RestSecurityClientOkHttp(this);
   }

   CompletionStage<RestResponse> execute(Request.Builder builder) {
      Request request = builder.build();
      log.tracef("Request %s", request);
      CompletableFuture<RestResponse> response = new CompletableFuture<>();
      httpClient.newCall(request).enqueue(new Callback() {
         @Override
         public void onFailure(Call call, IOException e) {
            if (e.getSuppressed().length > 0 && e.getSuppressed()[0] instanceof AbstractAuthenticator.AuthenticationException) {
               Throwable t = e.getSuppressed()[0];
               Response.Builder builder = new Response.Builder()
                     .code(403)
                     .body(ResponseBody.create(MediaType.parse("text/plain"), t.getMessage()))
                     .request(call.request())
                     .message(t.getMessage())
                     .protocol(Protocol.HTTP_1_1); // Doesn't really matter
               log.trace("Response unauthorized", t);
               response.complete(new RestResponseOkHttp(builder.build()));
            } else {
               log.trace("Response error", e);
               response.completeExceptionally(e);
            }
         }

         @Override
         public void onResponse(Call call, Response r) {
            log.tracef("Response %s", r);
            response.complete(new RestResponseOkHttp(r));
         }
      });
      return response;
   }

   CompletionStage<RestResponse> execute(String basePath, String... subPaths) {
      return execute(new Request.Builder(), basePath, subPaths);
   }

   CompletionStage<RestResponse> execute(Request.Builder builder, String basePath, String... subPaths) {
      if (subPaths != null) {
         StringBuilder sb = new StringBuilder(basePath);
         for (String subPath : subPaths) {
            if (subPath == null) {
               continue;
            }
            if (!subPath.startsWith("/")) {
               sb.append("/");
            }
            sb.append(subPath);
         }
         builder.url(sb.toString());
      } else {
         builder.url(basePath);
      }
      return execute(builder);
   }

   String getBaseURL() {
      return baseURL;
   }

   OkHttpClient client() {
      return httpClient;
   }
}
