package org.infinispan.client.openapi.configuration;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;

import org.infinispan.client.openapi.ApiClient;
import org.infinispan.client.openapi.impl.jdk.OpenAPIClientThreadFactory;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configuration.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
@BuiltBy(OpenAPIClientConfigurationBuilder.class)
public class OpenAPIClientConfiguration {
   public static final AtomicLong CLIENT_IDS = new AtomicLong();
   private final long connectionTimeout;
   private final List<ServerConfiguration> servers;
   private final long socketTimeout;
   private final SecurityConfiguration security;
   private final boolean tcpNoDelay;
   private final boolean tcpKeepAlive;
   private final Protocol protocol;
   private final String contextPath;
   private final boolean priorKnowledge;
   private final boolean followRedirects;
   private final Map<String, String> headers;
   private final ExecutorService executorService;
   private final boolean pingOnCreate;

   OpenAPIClientConfiguration(List<ServerConfiguration> servers, Protocol protocol, long connectionTimeout, long socketTimeout, SecurityConfiguration security, boolean tcpNoDelay, boolean tcpKeepAlive, String contextPath, boolean priorKnowledge, boolean followRedirects, boolean pingOnCreate, Map<String, String> headers, ExecutorService executorService) {
      this.servers = Collections.unmodifiableList(servers);
      this.protocol = protocol;
      this.connectionTimeout = connectionTimeout;
      this.socketTimeout = socketTimeout;
      this.security = security;
      this.tcpNoDelay = tcpNoDelay;
      this.tcpKeepAlive = tcpKeepAlive;
      this.contextPath = contextPath;
      this.priorKnowledge = priorKnowledge;
      this.followRedirects = followRedirects;
      this.headers = headers;
      this.executorService = executorService;
      this.pingOnCreate = pingOnCreate;
   }

   public Protocol protocol() {
      return protocol;
   }

   public boolean priorKnowledge() {
      return priorKnowledge;
   }

   public boolean followRedirects() {
      return followRedirects;
   }

   public long connectionTimeout() {
      return connectionTimeout;
   }

   public List<ServerConfiguration> servers() {
      return servers;
   }

   public long socketTimeout() {
      return socketTimeout;
   }

   public SecurityConfiguration security() {
      return security;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay;
   }

   public boolean tcpKeepAlive() {
      return tcpKeepAlive;
   }

   public String contextPath() {
      return contextPath;
   }

   public Map<String, String> headers() {
      return headers;
   }

   public ExecutorService executorService() {
      return executorService;
   }

   public Properties properties() {
      TypedProperties properties = new TypedProperties();
      properties.setProperty(OpenAPIClientConfigurationProperties.PROTOCOL, protocol().name());
      properties.setProperty(OpenAPIClientConfigurationProperties.CONNECT_TIMEOUT, Long.toString(connectionTimeout()));
      properties.setProperty(OpenAPIClientConfigurationProperties.SO_TIMEOUT, socketTimeout());
      properties.setProperty(OpenAPIClientConfigurationProperties.TCP_NO_DELAY, tcpNoDelay());
      properties.setProperty(OpenAPIClientConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive());
      properties.setProperty(OpenAPIClientConfigurationProperties.CONTEXT_PATH, contextPath());
      properties.setProperty(OpenAPIClientConfigurationProperties.USER_AGENT, headers.get("User-Agent"));
      properties.setProperty(OpenAPIClientConfigurationProperties.PING_ON_CREATE, pingOnCreate());

      StringBuilder servers = new StringBuilder();
      for (ServerConfiguration server : servers()) {
         if (!servers.isEmpty()) {
            servers.append(";");
         }
         servers.append(server.host()).append(":").append(server.port());
      }
      properties.setProperty(OpenAPIClientConfigurationProperties.SERVER_LIST, servers.toString());

      properties.setProperty(OpenAPIClientConfigurationProperties.USE_SSL, Boolean.toString(security.ssl().enabled()));

      if (security.ssl().keyStoreFileName() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.KEY_STORE_FILE_NAME, security.ssl().keyStoreFileName());

      if (security.ssl().keyStorePassword() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.KEY_STORE_PASSWORD, new String(security.ssl().keyStorePassword()));

      if (security.ssl().trustStoreFileName() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.TRUST_STORE_FILE_NAME, security.ssl().trustStoreFileName());

      if (security.ssl().trustStorePassword() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.TRUST_STORE_PASSWORD, new String(security.ssl().trustStorePassword()));

      if (security.ssl().sniHostName() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.SNI_HOST_NAME, security.ssl().sniHostName());

      if (security.ssl().protocol() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.SSL_PROTOCOL, security.ssl().protocol());

      if (security.ssl().sslContext() != null)
         properties.put(OpenAPIClientConfigurationProperties.SSL_CONTEXT, security.ssl().sslContext());

      if (security.ssl().trustManagers() != null)
         properties.put(OpenAPIClientConfigurationProperties.TRUST_MANAGERS, security.ssl().trustManagers());

      properties.setProperty(OpenAPIClientConfigurationProperties.USE_AUTH, Boolean.toString(security.authentication().enabled()));

      if (security.authentication().mechanism() != null)
         properties.setProperty(OpenAPIClientConfigurationProperties.AUTH_MECHANISM, security.authentication().mechanism());

      return properties;
   }

   public boolean pingOnCreate() {
      return pingOnCreate;
   }

   public String toURI() {
      StringBuilder sb = new StringBuilder("http");
      if (security.ssl().enabled()) {
         sb.append('s');
      }
      sb.append("://");
      if (security.authentication().enabled()) {
         sb.append(security.authentication().username()).append(':').append(security.authentication().password()).append('@');
      }
      for (int i = 0; i < servers.size(); i++) {
         if (i > 0) {
            sb.append(";");
         }
         ServerConfiguration server = servers.get(i);
         sb.append(server.host()).append(":").append(server.port());
      }

      return sb.toString();
   }

   @Override
   public String toString() {
      return "OpenAPIClientConfiguration{" +
            "connectionTimeout=" + connectionTimeout +
            ", servers=" + servers +
            ", socketTimeout=" + socketTimeout +
            ", tcpNoDelay=" + tcpNoDelay +
            ", tcpKeepAlive=" + tcpKeepAlive +
            ", pingOnCreate=" + pingOnCreate +
            ", protocol=" + protocol +
            ", contextPath='" + contextPath + '\'' +
            ", priorKnowledge=" + priorKnowledge +
            ", followRedirects=" + followRedirects +
            ", headers=" + headers +
            ", security=" + security +
            '}';
   }

   ApiClient apiClient() {
      HttpClient.Builder builder = HttpClient.newBuilder();
      ExecutorService executorService = this.executorService;
      if (executorService == null) {
         executorService = new ThreadPoolExecutor(0, 10,
               60L, TimeUnit.SECONDS,
               new LinkedBlockingQueue<>(),
               new OpenAPIClientThreadFactory(CLIENT_IDS.incrementAndGet()));
      }
      builder
            .connectTimeout(Duration.ofMillis(connectionTimeout()))
            .followRedirects(followRedirects() ? HttpClient.Redirect.ALWAYS : HttpClient.Redirect.NEVER);
      builder.executor(executorService);
      SslConfiguration ssl = security().ssl();
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
      switch (protocol) {
         case HTTP_11:
            builder.version(HttpClient.Version.HTTP_1_1);
            break;
         case HTTP_20:
            builder.version(HttpClient.Version.HTTP_2);
            break;
      }
//      AuthenticationConfiguration authentication = security().authentication();
//      AutoDetectAuthenticator authenticator;
//      if (authentication.enabled()) {
//         authenticator = switch (authentication.mechanism()) {
//            case "AUTO" -> new AutoDetectAuthenticator(httpClient, authentication);
//            case "SPNEGO" -> new NegotiateAuthenticator(httpClient, authentication);
//            case "DIGEST" -> new DigestAuthenticator(httpClient, authentication);
//            case "BASIC" -> new BasicAuthenticator(httpClient, authentication);
//            case "BEARER_TOKEN" -> new BearerAuthenticator(httpClient, authentication);
//            default -> throw new IllegalArgumentException("Cannot handle " + authentication.mechanism());
//         };
//      } else {
//         authenticator = null;
//      }
      ServerConfiguration server = servers().get(0);
      String baseURL = String.format("%s://%s:%d", ssl.enabled() ? "https" : "http", server.host(), server.port());
//      if (pingOnCreate) {
//         try {
//            head("/").toCompletableFuture().get();
//         } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException(e);
//         }
//      }
      return new ApiClient(builder, ApiClient.createDefaultObjectMapper(), baseURL);
   }
}
