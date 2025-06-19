package org.infinispan.client.openapi;

import static org.infinispan.client.openapi.configuration.OpenAPIClientConfiguration.CLIENT_IDS;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.infinispan.client.openapi.api.CacheApi;
import org.infinispan.client.openapi.configuration.AuthenticationConfiguration;
import org.infinispan.client.openapi.configuration.OpenAPIClientConfiguration;
import org.infinispan.client.openapi.configuration.ServerConfiguration;
import org.infinispan.client.openapi.configuration.SslConfiguration;
import org.infinispan.client.openapi.impl.jdk.OpenAPIClientThreadFactory;
import org.infinispan.client.openapi.impl.jdk.auth.AutoDetectAuthenticator;
import org.infinispan.client.openapi.impl.jdk.auth.BasicAuthenticator;
import org.infinispan.client.openapi.impl.jdk.auth.BearerAuthenticator;
import org.infinispan.client.openapi.impl.jdk.auth.DigestAuthenticator;
import org.infinispan.client.openapi.impl.jdk.auth.HttpAuthenticator;
import org.infinispan.client.openapi.impl.jdk.auth.NegotiateAuthenticator;
import org.infinispan.commons.util.SslContextFactory;

public class OpenAPIClient implements AutoCloseable {
   private final OpenAPIClientConfiguration configuration;
   private final ApiClient apiClient;
   private final boolean managedExecutorService;
   private final ExecutorService executorService;
   private final HttpAuthenticator authenticator;

   public OpenAPIClient(OpenAPIClientConfiguration configuration) {
      this.configuration = configuration;
      HttpClient.Builder builder = HttpClient.newBuilder();
      ExecutorService executorService = configuration.executorService();
      if (executorService == null) {
         executorService = new ThreadPoolExecutor(0, 10,
               60L, TimeUnit.SECONDS,
               new LinkedBlockingQueue<>(),
               new OpenAPIClientThreadFactory(CLIENT_IDS.incrementAndGet()));
         managedExecutorService = true;
      } else {
         managedExecutorService = false;
      }
      this.executorService = executorService;
      builder
            .connectTimeout(Duration.ofMillis(configuration.connectionTimeout()))
            .followRedirects(configuration.followRedirects() ? HttpClient.Redirect.ALWAYS : HttpClient.Redirect.NEVER);
      builder.executor(executorService);
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
                  .classLoader(Thread.currentThread().getContextClassLoader());
            sslContext = sslContextFactory.build().sslContext();
         }
         builder.sslContext(sslContext);
      }
      switch (configuration.protocol()) {
         case HTTP_11:
            builder.version(HttpClient.Version.HTTP_1_1);
            break;
         case HTTP_20:
            builder.version(HttpClient.Version.HTTP_2);
            break;
      }
      ServerConfiguration server = configuration.servers().get(0);
      apiClient = new ApiClient(builder, ApiClient.createDefaultObjectMapper(), String.format("%s://%s:%d", ssl.enabled() ? "https" : "http", server.host(), server.port()));
      apiClient.setConnectTimeout(Duration.ofMillis(configuration.connectionTimeout()));
      apiClient.setReadTimeout(Duration.ofMillis(configuration.socketTimeout()));
      HttpClient httpClient = apiClient.getHttpClient();
      AuthenticationConfiguration authentication = configuration.security().authentication();
      if (authentication.enabled()) {
         switch (authentication.mechanism()) {
            case "AUTO":
               authenticator = new AutoDetectAuthenticator(httpClient, authentication);
               break;
            case "SPNEGO":
               authenticator = new NegotiateAuthenticator(httpClient, authentication);
               break;
            case "DIGEST":
               authenticator = new DigestAuthenticator(httpClient, authentication);
               break;
            case "BASIC":
               authenticator = new BasicAuthenticator(httpClient, authentication);
               break;
            case "BEARER_TOKEN":
               authenticator = new BearerAuthenticator(httpClient, authentication);
               break;
            default:
               throw new IllegalArgumentException("Cannot handle " + authentication.mechanism());
         }
      } else {
         authenticator = null;
      }
      apiClient.setRequestInterceptor(b -> {
         configuration.headers().forEach(b::header);
         if (authenticator != null && authenticator.supportsPreauthentication()) {
            authenticator.preauthenticate(b);
         }
      });
      apiClient.setResponseInterceptor(r -> {
         if (r.statusCode() == 401 && authenticator != null) {
            // TODO
         }
      });
      if (configuration.pingOnCreate()) {
         try {
            httpClient.send(HttpRequest.newBuilder(URI.create(apiClient.basePath)).build(), HttpResponse.BodyHandlers.discarding());
         } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
         }
      }
   }

   public CacheApi cache() {
      return new CacheApi(apiClient);
   }

   public static OpenAPIClient forConfiguration(OpenAPIClientConfiguration configuration) {
      return new OpenAPIClient(configuration);
   }

   @Override
   public void close() throws Exception {
      if (Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0) {
         ((AutoCloseable) apiClient.getHttpClient()).close(); // close() was only introduced in JDK 21
      }
      if (managedExecutorService) {
         executorService.shutdownNow();
      }
   }

   public OpenAPIClientConfiguration getConfiguration() {
      return configuration;
   }
}
