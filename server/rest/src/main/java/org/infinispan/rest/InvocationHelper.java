package org.infinispan.rest;


import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.util.concurrent.BlockingManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

/**
 * @since 10.0
 */
public class InvocationHelper {
   private final ObjectMapper mapper= new ObjectMapper();
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final JsonReader jsonReader = new JsonReader();
   private final JsonWriter jsonWriter = new JsonWriter();
   private final RestCacheManager<Object> restCacheManager;
   private final EmbeddedCounterManager counterManager;
   private final RestServerConfiguration configuration;
   private final ServerManagement server;
   private final BlockingManager manager;

   InvocationHelper(RestCacheManager<Object> restCacheManager, EmbeddedCounterManager counterManager,
                    RestServerConfiguration configuration, ServerManagement server, BlockingManager manager) {
      this.restCacheManager = restCacheManager;
      this.counterManager = counterManager;
      this.configuration = configuration;
      this.server = server;
      this.manager = manager;
      this.mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE).registerModule(new Jdk8Module());
   }

   public ObjectMapper getMapper() {
      return mapper;
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

   public BlockingManager getManager() {
      return manager;
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
