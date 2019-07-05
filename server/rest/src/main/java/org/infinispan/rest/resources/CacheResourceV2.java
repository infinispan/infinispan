package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadPoolExecutor;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.JsonReader;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.stats.Stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;

public class CacheResourceV2 extends CacheResource {

   private static final JsonReader JSON_READER = new JsonReader();
   private static final JsonWriter JSON_WRITER = new JsonWriter();
   private static final ParserRegistry PARSER_REGISTRY = new ParserRegistry();
   private final ObjectMapper mapper;


   public CacheResourceV2(RestCacheManager<Object> restCacheManager, RestServerConfiguration serverConfig,
                          ObjectMapper mapper, ThreadPoolExecutor executor) {
      super(restCacheManager, serverConfig, executor);
      this.mapper = mapper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Key related operations
            .invocation().methods(PUT, POST).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::putValueToCache)
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::getCacheValue)
            .invocation().method(DELETE).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::deleteCacheValue)

            // Info and statistics
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}/config").handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/stats").handleWith(this::getCacheStats)

            // Cache lifecycle
            .invocation().methods(POST).path("/v2/caches/{cacheName}").handleWith(this::createCache)
            .invocation().method(DELETE).path("/v2/caches/{cacheName}").handleWith(this::removeCache)

            // Operations
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("clear").handleWith(this::clearEntireCache)

            // Search
            .invocation().methods(GET, POST).path("/v2/caches/{cacheName}").withAction("search").handleWith(queryAction::search)
            .create();

   }

   private CompletionStage<RestResponse> removeCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = restCacheManager.getCache(cacheName, request.getSubject());
      if (cache == null) {
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      return CompletableFuture.supplyAsync(() -> {
         restCacheManager.getInstance().administration().removeCache(cacheName);
         responseBuilder.status(OK);
         return responseBuilder.build();
      }, executor);
   }

   private CompletableFuture<RestResponse> createCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      ContentSource contents = request.contents();
      String cacheName = request.variables().get("cacheName");
      byte[] bytes = contents.rawContent();
      if (bytes == null || bytes.length == 0) {
         return CompletableFuture.supplyAsync(() -> {
            restCacheManager.getInstance().administration().createCache(cacheName, (String) null);
            responseBuilder.status(OK);
            return responseBuilder.build();
         }, executor);
      }
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();

      MediaType sourceType = request.contentType() == null ? APPLICATION_JSON : request.contentType();

      if (sourceType.match(APPLICATION_JSON)) {
         JSON_READER.readJson(cfgBuilder, StandardConversions.convertTextToObject(bytes, sourceType));
      } else if (sourceType.match(MediaType.APPLICATION_XML)) {
         ConfigurationBuilderHolder builderHolder = PARSER_REGISTRY.parse(new String(bytes, UTF_8));
         cfgBuilder = builderHolder.getCurrentConfigurationBuilder();
      } else {
         responseBuilder.status(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }

      ConfigurationBuilder finalCfgBuilder = cfgBuilder;
      return CompletableFuture.supplyAsync(() -> {
         restCacheManager.getInstance().administration().createCache(cacheName, finalCfgBuilder.build());

         responseBuilder.status(OK);
         return responseBuilder.build();
      }, executor);
   }

   private CompletionStage<RestResponse> getCacheStats(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = restCacheManager.getCache(cacheName, request.getSubject());
      Stats stats = cache.getAdvancedCache().getStats();
      try {
         byte[] statsResponse = mapper.writeValueAsBytes(stats);
         responseBuilder.contentType(APPLICATION_JSON)
               .entity(statsResponse)
               .status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> getCacheConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");

      MediaType accept = ConfigResource.getAccept(request);
      responseBuilder.contentType(accept);

      Cache<?, ?> cache = restCacheManager.getCache(cacheName, request.getSubject());
      if (cache == null)
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).build());

      Configuration cacheConfiguration = cache.getCacheConfiguration();

      String entity;
      if (accept.getTypeSubtype().equals(APPLICATION_XML_TYPE)) {
         entity = cacheConfiguration.toXMLString();
      } else {
         entity = JSON_WRITER.toJSON(cacheConfiguration);
      }
      return CompletableFuture.completedFuture(responseBuilder.status(OK).entity(entity).build());
   }

}
