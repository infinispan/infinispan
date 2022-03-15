package org.infinispan.rest;

import java.util.concurrent.Executor;

import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metrics.impl.MetricsCollector;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.server.core.ServerManagement;

/**
 * @since 10.0
 */
public class InvocationHelper {
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final RestCacheManager<Object> restCacheManager;
   private final EmbeddedCounterManager counterManager;
   private final RestServerConfiguration configuration;
   private final ServerManagement server;
   private final Executor executor;
   private final RestServer protocolServer;
   private final EncoderRegistry encoderRegistry;
   private final MetricsCollector metricsCollector;

   InvocationHelper(RestServer protocolServer, RestCacheManager<Object> restCacheManager, EmbeddedCounterManager counterManager,
                    RestServerConfiguration configuration, ServerManagement server, Executor executor) {
      this.protocolServer = protocolServer;
      this.restCacheManager = restCacheManager;
      this.counterManager = counterManager;
      this.configuration = configuration;
      this.server = server;
      this.executor = executor;

      GlobalComponentRegistry globalComponentRegistry = restCacheManager.getInstance().getGlobalComponentRegistry();
      this.encoderRegistry = globalComponentRegistry.getComponent(EncoderRegistry.class);
      this.metricsCollector = globalComponentRegistry.getComponent(MetricsCollector.class);
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
      return counterManager;
   }

   public String getContext() {
      return configuration.contextPath();
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

   private void checkServerStatus() {
      ComponentStatus status = server.getStatus();
      switch (status) {
         case STOPPING:
         case TERMINATED:
            throw new ServiceUnavailableException("Unable to process REST request when Server is " + status);
      }
   }
}
