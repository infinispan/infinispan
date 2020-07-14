package org.infinispan.rest;

import java.util.concurrent.Executor;

import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.core.ServerManagement;

/**
 * @since 10.0
 */
public class InvocationHelper {
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final JsonReader jsonReader = new JsonReader();
   private final JsonWriter jsonWriter = new JsonWriter();
   private final RestCacheManager<Object> restCacheManager;
   private final EmbeddedCounterManager counterManager;
   private final RestServerConfiguration configuration;
   private final ServerManagement server;
   private final Executor executor;

   InvocationHelper(RestCacheManager<Object> restCacheManager, EmbeddedCounterManager counterManager,
                    RestServerConfiguration configuration, ServerManagement server, Executor executor) {
      this.restCacheManager = restCacheManager;
      this.counterManager = counterManager;
      this.configuration = configuration;
      this.server = server;
      this.executor = executor;
   }

   public ParserRegistry getParserRegistry() {
      return parserRegistry;
   }

   public JsonReader getJsonReader() {
      return jsonReader;
   }

   public RestCacheManager<Object> getRestCacheManager() {
      return restCacheManager;
   }

   public RestServerConfiguration getConfiguration() {
      return configuration;
   }

   public JsonWriter getJsonWriter() {
      return jsonWriter;
   }

   public Executor getExecutor() {
      return executor;
   }

   public ServerManagement getServer() {
      return server;
   }

   public EmbeddedCounterManager getCounterManager() {
      return counterManager;
   }

   public String getContext() {
      return configuration.contextPath();
   }
}
