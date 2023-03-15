package org.infinispan.rest;

import java.util.concurrent.Executor;

import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metrics.impl.MetricsCollector;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.server.core.ServerManagement;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.0
 */
public class InvocationHelper {
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final RestCacheManager<Object> restCacheManager;
   private final EmbeddedCounterManager counterManager;
   private final ServerManagement server;
   private final Executor executor;
   private final RestServer protocolServer;
   private final EncoderRegistry encoderRegistry;
   private final MetricsCollector metricsCollector;
   private final ProtobufMetadataManager protobufMetadataManager;
   private boolean ssl;

   InvocationHelper(RestServer protocolServer, RestCacheManager<Object> restCacheManager, EmbeddedCounterManager counterManager,
                    ServerManagement server, Executor executor) {
      this.protocolServer = protocolServer;
      this.restCacheManager = restCacheManager;
      this.counterManager = counterManager;
      this.server = server;
      this.executor = executor;

      GlobalComponentRegistry globalComponentRegistry = restCacheManager.getInstance().getGlobalComponentRegistry();
      this.encoderRegistry = globalComponentRegistry.getComponent(EncoderRegistry.class);
      this.metricsCollector = globalComponentRegistry.getComponent(MetricsCollector.class);
      this.protobufMetadataManager = globalComponentRegistry.getComponent(ProtobufMetadataManager.class);
      ssl = protocolServer.getConfiguration().ssl().enabled();
   }

   public ParserRegistry getParserRegistry() {
      return parserRegistry;
   }

   public RestCacheManager<Object> getRestCacheManager() {
      checkServerStatus();
      return restCacheManager;
   }

   public RestServerConfiguration getConfiguration() {
      return protocolServer.getConfiguration();
   }

   public Executor getExecutor() {
      return executor;
   }

   public ServerManagement getServer() {
      return server;
   }

   public EmbeddedCounterManager getCounterManager() {
      checkServerStatus();
      return counterManager;
   }

   public String getContext() {
      return protocolServer.getConfiguration().contextPath();
   }

   public RestServer getProtocolServer() {
      return protocolServer;
   }

   public EncoderRegistry getEncoderRegistry() {
      return encoderRegistry;
   }

   public MetricsCollector getMetricsCollector() {
      return metricsCollector;
   }

   public ProtobufMetadataManager protobufMetadataManager() {
      return protobufMetadataManager;
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
      return new NettyRestResponse.Builder(request, ssl);
   }

   public NettyRestResponse.Builder newResponse(RestRequest request) {
      return new NettyRestResponse.Builder(request, ssl);
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

   void setSsl(boolean ssl) {
      this.ssl = ssl;
   }
}
