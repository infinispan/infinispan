package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.rest.CacheInputStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.stats.Stats;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class CacheResourceV2 extends CacheResource {

   private static final int STREAM_BATCH_SIZE = 1000;

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
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("keys").handleWith(this::streamKeys)

            // Info and statistics
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}").withAction("config").handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("stats").handleWith(this::getCacheStats)

            // List
            .invocation().methods(GET).path("/v2/caches/").handleWith(this::getCacheNames)

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

   private CompletionStage<RestResponse> streamKeys(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");

      List<String> values = request.parameters().get("batch");
      int batch = values == null || values.isEmpty() ? STREAM_BATCH_SIZE : Integer.parseInt(values.iterator().next());

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, APPLICATION_JSON, APPLICATION_JSON, request);
      if (cache == null) {
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      responseBuilder.entity(new CacheInputStream(cache.keySet().stream(), batch));

      responseBuilder.contentType(APPLICATION_JSON_TYPE);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> removeCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder().status(NO_CONTENT);
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      Cache<?, ?> cache = restCacheManager.getCache(cacheName, request);
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
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder().status(NO_CONTENT);
      List<String> template = request.parameters().get("template");
      String cacheName = request.variables().get("cacheName");

      EnumSet<AdminFlag> adminFlags = request.getAdminFlags();
      EmbeddedCacheManagerAdmin initialAdmin = invocationHelper.getRestCacheManager().getInstance().administration();
      EmbeddedCacheManagerAdmin administration = adminFlags == null ? initialAdmin : initialAdmin.withFlags(adminFlags);

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
      } else if (sourceType.match(APPLICATION_XML)) {
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
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
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

      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML);
      responseBuilder.contentType(accept);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
      }
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
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

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

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

   private CompletionStage<RestResponse> getCacheNames(RestRequest request) throws RestResponseException {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      try {
         byte[] bytes = invocationHelper.getMapper().writeValueAsBytes(invocationHelper.getRestCacheManager().getCacheNames());
         responseBuilder.contentType(APPLICATION_JSON).entity(bytes).status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return completedFuture(responseBuilder.build());
   }

}
