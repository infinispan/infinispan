package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.stats.Stats;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class CacheResourceV2 extends CacheResource {

   public CacheResourceV2(InvocationHelper invocationHelper) {
      super(invocationHelper);
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
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("size").handleWith(this::getSize)

            // Search
            .invocation().methods(GET, POST).path("/v2/caches/{cacheName}").withAction("search").handleWith(queryAction::search)
            .create();

   }

   private CompletionStage<RestResponse> removeCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      Cache<?, ?> cache = restCacheManager.getCache(cacheName, request.getSubject());
      if (cache == null) {
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      return CompletableFuture.supplyAsync(() -> {
         restCacheManager.getInstance().administration().removeCache(cacheName);
         responseBuilder.status(OK);
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletableFuture<RestResponse> createCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      List<String> template = request.parameters().get("template");
      String cacheName = request.variables().get("cacheName");
      EmbeddedCacheManagerAdmin administration = invocationHelper.getRestCacheManager().getInstance().administration();
      if (template != null && !template.isEmpty()) {
         String templateName = template.iterator().next();
         return CompletableFuture.supplyAsync(() -> {
            administration.createCache(cacheName, templateName);
            responseBuilder.status(OK);
            return responseBuilder.build();
         }, invocationHelper.getExecutor());
      }

      ContentSource contents = request.contents();
      byte[] bytes = contents.rawContent();
      if (bytes == null || bytes.length == 0) {
         return CompletableFuture.supplyAsync(() -> {
            administration.createCache(cacheName, (String) null);
            responseBuilder.status(OK);
            return responseBuilder.build();
         }, invocationHelper.getExecutor());
      }
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();

      MediaType sourceType = request.contentType() == null ? APPLICATION_JSON : request.contentType();

      if (sourceType.match(APPLICATION_JSON)) {
         invocationHelper.getJsonReader().readJson(cfgBuilder, StandardConversions.convertTextToObject(bytes, sourceType));
      } else if (sourceType.match(MediaType.APPLICATION_XML)) {
         ConfigurationBuilderHolder builderHolder = invocationHelper.getParserRegistry().parse(new String(bytes, UTF_8));
         cfgBuilder = builderHolder.getCurrentConfigurationBuilder();
      } else {
         responseBuilder.status(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }

      ConfigurationBuilder finalCfgBuilder = cfgBuilder;
      return CompletableFuture.supplyAsync(() -> {
         administration.createCache(cacheName, finalCfgBuilder.build());

         responseBuilder.status(OK);
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getCacheStats(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request.getSubject());
      Stats stats = cache.getAdvancedCache().getStats();
      try {
         byte[] statsResponse = invocationHelper.getMapper().writeValueAsBytes(stats);
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

      MediaType accept = CacheManagerResource.getAccept(request);
      responseBuilder.contentType(accept);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
      }
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request.getSubject());
      if (cache == null)
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).build());

      Configuration cacheConfiguration = cache.getCacheConfiguration();

      String entity;
      if (accept.getTypeSubtype().equals(APPLICATION_XML_TYPE)) {
         entity = cacheConfiguration.toXMLString();
      } else {
         entity = invocationHelper.getJsonWriter().toJSON(cacheConfiguration);
      }
      return CompletableFuture.completedFuture(responseBuilder.status(OK).entity(entity).build());
   }

   private CompletionStage<RestResponse> getSize(RestRequest request) {
      String cacheName = request.variables().get("cacheName");

      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request.getSubject());

      return CompletableFuture.supplyAsync(() -> {
         try {
            int size = cache.size();
            responseBuilder.entity(invocationHelper.getMapper().writeValueAsBytes(size));
         } catch (JsonProcessingException e) {
            responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

}
