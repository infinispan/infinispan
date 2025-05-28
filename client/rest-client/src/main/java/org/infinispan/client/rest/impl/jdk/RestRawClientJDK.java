package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.RestClient.LOG;
import static org.infinispan.client.rest.RestHeaders.ACCEPT;
import static org.infinispan.client.rest.RestHeaders.ACCEPT_ENCODING;
import static org.infinispan.client.rest.RestHeaders.CONTENT_TYPE;

import java.io.Closeable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.ServerConfiguration;
import org.infinispan.client.rest.configuration.SslConfiguration;
import org.infinispan.client.rest.impl.jdk.auth.AutoDetectAuthenticator;
import org.infinispan.client.rest.impl.jdk.auth.BasicAuthenticator;
import org.infinispan.client.rest.impl.jdk.auth.BearerAuthenticator;
import org.infinispan.client.rest.impl.jdk.auth.DigestAuthenticator;
import org.infinispan.client.rest.impl.jdk.auth.HttpAuthenticator;
import org.infinispan.client.rest.impl.jdk.auth.NegotiateAuthenticator;
import org.infinispan.client.rest.impl.jdk.sse.EventSubscriber;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.SslContextFactory;

public class RestRawClientJDK implements RestRawClient, AutoCloseable {
   private static final AtomicLong CLIENT_IDS = new AtomicLong();
   private final RestClientConfiguration configuration;
   private final HttpAuthenticator authenticator;
   private final HttpClient httpClient;
   private final String baseURL;
   private final boolean managedExecutorService;
   private final ExecutorService executorService;

   RestRawClientJDK(RestClientConfiguration configuration) {
      this.configuration = configuration;
      HttpClient.Builder builder = HttpClient.newBuilder();
      ExecutorService executorService = configuration.executorService();
      if (executorService == null) {
         executorService = new ThreadPoolExecutor(0, 10,
               60L, TimeUnit.SECONDS,
               new LinkedBlockingQueue<>(),
               new RestClientThreadFactory(CLIENT_IDS.incrementAndGet()));
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
      httpClient = builder.build();
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
      ServerConfiguration server = configuration.servers().get(0);
      baseURL = String.format("%s://%s:%d", ssl.enabled() ? "https" : "http", server.host(), server.port());
      if (configuration.pingOnCreate()) {
         try {
            head("/").toCompletableFuture().get();
         } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
         }
      }
   }

   @Override
   public CompletionStage<RestResponse> post(String path, Map<String, String> headers, RestEntity entity) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      builder.POST(entity.bodyPublisher());
      if (entity.contentType() != null) {
         builder.header(CONTENT_TYPE, entity.contentType().toString());
      }
      return execute(builder, bodyHandlerSupplier(headers));
   }

   @Override
   public CompletionStage<RestResponse> put(String path, Map<String, String> headers, RestEntity entity) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      builder.PUT(entity.bodyPublisher());
      if (entity.contentType() != null) {
         builder.header(CONTENT_TYPE, entity.contentType().toString());
      }
      return execute(builder, bodyHandlerSupplier(headers));
   }

   @Override
   public CompletionStage<RestResponse> get(String path, Map<String, String> headers) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.GET().uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      return execute(builder, bodyHandlerSupplier(headers));
   }

   @Override
   public CompletionStage<RestResponse> get(String path, Map<String, String> headers, Supplier<HttpResponse.BodyHandler<?>> supplier) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.GET().uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      return execute(builder, supplier);
   }

   @Override
   public CompletionStage<RestResponse> delete(String path, Map<String, String> headers) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      builder.DELETE();
      return execute(builder, bodyHandlerSupplier(headers));
   }

   @Override
   public CompletionStage<RestResponse> options(String path, Map<String, String> headers) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      builder.method("OPTIONS", HttpRequest.BodyPublishers.noBody());
      return execute(builder, bodyHandlerSupplier(headers));
   }

   @Override
   public CompletionStage<RestResponse> head(String path, Map<String, String> headers) {
      HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(Duration.ofMillis(configuration.socketTimeout()));
      builder.uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      builder.method("HEAD", HttpRequest.BodyPublishers.noBody());
      return execute(builder, bodyHandlerSupplier(headers));
   }

   @Override
   public Closeable listen(String path, Map<String, String> headers, RestEventListener listener) {
      HttpRequest.Builder builder = HttpRequest.newBuilder();
      builder.uri(URI.create(baseURL + path));
      headers.forEach(builder::header);
      configuration.headers().forEach(builder::header);
      EventSubscriber subscriber = new EventSubscriber(listener);
      execute(builder, subscriber::bodyHandler).handle((r, t) -> {
         if (t != null) {
            listener.onError(t, r);
         } else {
            int status = r.status();
            if (status >= 300) {
               listener.onError(null, r);
            }
         }
         return null;
      });
      return subscriber;
   }

   private Supplier<HttpResponse.BodyHandler<?>> bodyHandlerSupplier(Map<String, String> headers) {
      String accept = headers.get(ACCEPT);
      String encoding = headers.get(ACCEPT_ENCODING);
      if (accept == null && encoding == null) {
         return HttpResponse.BodyHandlers::ofString;
      } else {
         if (encoding != null && !"identity".equals(encoding)) {
            return HttpResponse.BodyHandlers::ofByteArray;
         }
         MediaType mediaType = MediaType.parseList(accept).findFirst().get();
         return switch (mediaType.getTypeSubtype()) {
            case MediaType.APPLICATION_OCTET_STREAM_TYPE, MediaType.APPLICATION_PROTOSTREAM_TYPE,
                 MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE -> HttpResponse.BodyHandlers::ofByteArray;
            case MediaType.APPLICATION_GZIP_TYPE -> HttpResponse.BodyHandlers::ofInputStream;
            default -> HttpResponse.BodyHandlers::ofString;
         };
      }
   }

   private <T> CompletionStage<RestResponse> execute(HttpRequest.Builder builder, Supplier<HttpResponse.BodyHandler<?>> handlerSupplier) {
      // Add configured headers
      configuration.headers().forEach(builder::header);
      HttpRequest request = builder.build();
      LOG.tracef("Request %s", request);
      if (authenticator != null && authenticator.supportsPreauthentication()) {
         authenticator.preauthenticate(builder);
      }
      return handle(httpClient.sendAsync(request, handlerSupplier.get()), handlerSupplier).thenApply(RestResponseJDK::new);
   }

   private <T> CompletionStage<HttpResponse<T>> handle(CompletionStage<HttpResponse<T>> response, Supplier<HttpResponse.BodyHandler<?>> handlerSupplier) {
      return response.thenCompose(r -> {
         if (r.statusCode() == 401 && authenticator != null) {
            CompletionStage<HttpResponse<T>> authenticate = authenticator.authenticate(r, handlerSupplier.get());
            if (authenticate == null) {
               // The authenticator has given up, return the failure as is
               return CompletableFuture.completedFuture(r);
            } else {
               return handle(authenticate, handlerSupplier);
            }
         } else {
            return CompletableFuture.completedFuture(r);
         }
      });
   }

   @Override
   public void close() throws Exception {
      if (Runtime.version().compareTo(Runtime.Version.parse("21")) >= 0) {
         ((AutoCloseable) httpClient).close(); // close() was only introduced in JDK 21
      }
      if (managedExecutorService) {
         executorService.shutdownNow();
      }
   }

   RestClientConfiguration getConfiguration() {
      return configuration;
   }
}
