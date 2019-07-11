package org.infinispan.rest;

import java.util.concurrent.Executor;

import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @since 10.0
 */
public class InvocationHelper {
   private final ObjectMapper mapper = new ObjectMapper();
   private final ParserRegistry parserRegistry = new ParserRegistry();
   private final JsonReader jsonReader = new JsonReader();
   private final JsonWriter jsonWriter = new JsonWriter();
   private final RestCacheManager<Object> restCacheManager;
   private final EmbeddedCounterManager counterManager;
   private final RestServerConfiguration configuration;
   private final Executor executor;

   public InvocationHelper(RestCacheManager<Object> restCacheManager, EmbeddedCounterManager counterManager,
                           RestServerConfiguration configuration, Executor executor) {
      this.restCacheManager = restCacheManager;
      this.counterManager = counterManager;
      this.configuration = configuration;
      this.executor = executor;
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

   public Executor getExecutor() {
      return executor;
   }

   public EmbeddedCounterManager getCounterManager() {
      return counterManager;
   }
}
