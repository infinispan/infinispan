package org.infinispan.rest;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.Executor;

import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metrics.impl.MetricsRegistry;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.query.ProtobufMetadataManager;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.0
 */
public class InvocationHelper {
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final RestCacheManager<Object> restCacheManager;
   private final RestServerConfiguration configuration;
   private final ServerManagement server;
   private final Executor executor;
   private final RestServer protocolServer;
   private final String cspHeader;
   private EncoderRegistry encoderRegistry;
   private MetricsRegistry metricsRegistry;
   private ProtobufMetadataManager protobufMetadataManager;

   public InvocationHelper(RestServer protocolServer, RestCacheManager<Object> restCacheManager,
                    RestServerConfiguration configuration, ServerManagement server, Executor executor) {
      this.protocolServer = protocolServer;
      this.restCacheManager = restCacheManager;
      this.configuration = configuration;
      this.server = server;
      this.executor = executor;
      String url = server != null ? server.getLoginConfiguration(protocolServer).get(ServerManagement.URL) : "";
      String baseAuthUrl = createURLForCSPHeader(url);
      cspHeader = String.format("default-src 'self' %s data:; style-src 'self' 'unsafe-inline'; base-uri 'self'; form-action 'self'; frame-src 'self' %s; frame-ancestors 'self'; object-src 'none'; report-uri 'self';", baseAuthUrl, baseAuthUrl);
   }

   static String createURLForCSPHeader(String url) {
      String baseAuthUrl = "";
      if (url != null) {
         URI uri = URI.create(url);
         if (uri.getScheme() == null) {
            return baseAuthUrl;
         }

         baseAuthUrl = uri.getScheme() + "://" + uri.getHost();
         if (uri.getPort() > 0) {
            baseAuthUrl += ":" + uri.getPort();
         }
      }
      return baseAuthUrl;
   }

   public void postStart() {
      GlobalComponentRegistry globalComponentRegistry = GlobalComponentRegistry.of(restCacheManager.getInstance());
      this.encoderRegistry = globalComponentRegistry.getComponent(EncoderRegistry.class);
      this.metricsRegistry = globalComponentRegistry.getComponent(MetricsRegistry.class);
      this.protobufMetadataManager = globalComponentRegistry.getComponent(ProtobufMetadataManager.class);
   }

   public ParserRegistry getParserRegistry() {
      return parserRegistry;
   }

   public RestCacheManager<Object> getRestCacheManager() {
      checkServerStatus();
      return restCacheManager;
   }

   public RestServerConfiguration getConfiguration() {
      return configuration;
   }

   public Executor getExecutor() {
      return executor;
   }

   public ServerManagement getServer() {
      return server;
   }

   public EmbeddedCounterManager getCounterManager() {
      checkServerStatus();
      return (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(protocolServer.getCacheManager());
   }

   public String getContext() {
      return configuration.contextPath();
   }

   public RestServer getProtocolServer() {
      return protocolServer;
   }

   public EncoderRegistry getEncoderRegistry() {
      return Objects.requireNonNull(encoderRegistry, "Encoder registry not initialized yet");
   }

   public MetricsRegistry getMetricsRegistry() {
      return Objects.requireNonNull(metricsRegistry, "Metrics registry not initialized yet");
   }

   public ProtobufMetadataManager protobufMetadataManager() {
      return Objects.requireNonNull(protobufMetadataManager, "Protobuf manager not initialized yet");
   }

   private void checkServerStatus() {
      ComponentStatus status = server.getStatus();
      switch (status) {
         case STOPPING:
         case TERMINATED:
            throw new ServiceUnavailableException("Unable to process REST request when Server is " + status);
      }
   }

   public NettyRestResponse.Builder newResponse(FullHttpRequest request) {
      return newResponse(request.headers().get(RequestHeader.USER_AGENT.toString()), request.uri());
   }

   public NettyRestResponse.Builder newResponse(RestRequest request) {
      return newResponse(request.header(RequestHeader.USER_AGENT.toString()), request.uri());
   }

   private NettyRestResponse.Builder newResponse(String userAgent, String uri) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      // All browser's user agents start with "Mozilla"
      if (userAgent != null && userAgent.startsWith("Mozilla")) {
         builder.header("X-Frame-Options", "sameorigin").header("X-XSS-Protection", "1; mode=block").
               header("X-Content-Type-Options", "nosniff").
               header("Content-Security-Policy", cspHeader);
         // Only if we are using HTTPS
         if (configuration.ssl().enabled() || uri.startsWith("https")) {
            builder.header("Strict-Transport-Security", "max-age=31536000 ; includeSubDomains");
         }
      }
      return builder;
   }

   public NettyRestResponse newResponse(RestRequest request, HttpResponseStatus status) {
      return newResponse(request, status, null);
   }

   public NettyRestResponse newResponse(RestRequest request, HttpResponseStatus status, Object entity) {
      return newResponse(request)
            .status(status)
            .entity(entity)
            .build();
   }

   public NettyRestResponse noContentResponse(RestRequest request) {
      return newResponse(request, HttpResponseStatus.NO_CONTENT);
   }

   public NettyRestResponse notFoundResponse(RestRequest request) {
      return newResponse(request, HttpResponseStatus.NOT_FOUND);
   }
}
