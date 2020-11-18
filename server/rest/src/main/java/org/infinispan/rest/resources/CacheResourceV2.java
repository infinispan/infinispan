package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
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
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.notFoundResponseFuture;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.rest.CacheEntryInputStream;
import org.infinispan.rest.CacheKeyInputStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.stats.Stats;
import org.infinispan.upgrade.RollingUpgradeManager;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST resource to manage the caches.
 *
 * @since 10.0
 */
public class CacheResourceV2 extends BaseCacheResource implements ResourceHandler {

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
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("entries").handleWith(this::streamEntries)

            // Info and statistics
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}").withAction("config").handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("stats").handleWith(this::getCacheStats)

            // List
            .invocation().methods(GET).path("/v2/caches/").handleWith(this::getCacheNames)

            // Cache lifecycle
            .invocation().methods(POST).path("/v2/caches/{cacheName}").handleWith(this::createCache)
            .invocation().method(DELETE).path("/v2/caches/{cacheName}").handleWith(this::removeCache)
            .invocation().method(HEAD).path("/v2/caches/{cacheName}").handleWith(this::cacheExists)

            // Operations
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("clear").handleWith(this::clearEntireCache)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("size").handleWith(this::getSize)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("sync-data").handleWith(this::syncData)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("disconnect-source").handleWith(this::disconnectSource)

            // Search
            .invocation().methods(GET, POST).path("/v2/caches/{cacheName}").withAction("search").handleWith(queryAction::search)

            // Misc
            .invocation().methods(POST).path("/v2/caches").withAction("toJSON").handleWith(this::convertToJson)

            // All details
            .invocation().methods(GET).path("/v2/caches/{cacheName}").handleWith(this::getAllDetails)
            .create();

   }

   private CompletionStage<RestResponse> disconnectSource(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      builder.status(NO_CONTENT);

      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      try {
         upgradeManager.disconnectSource("hotrod");
      } catch (Exception e) {
         builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(e.getMessage());
      }
      return completedFuture(builder.build());
   }

   private CompletionStage<RestResponse> syncData(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      String readBatchReq = request.getParameter("read-batch");
      String threadsReq = request.getParameter("threads");

      int readBatch = readBatchReq == null ? 10_000 : Integer.parseInt(readBatchReq);
      if (readBatch < 1) {
         return CompletableFuture.completedFuture(builder.status(BAD_REQUEST).entity(Log.REST.illegalArgument("read-batch", readBatch).getMessage()).build());
      }
      int threads = request.getParameter("threads") == null ? ProcessorInfo.availableProcessors() : Integer.parseInt(threadsReq);
      if (threads < 1) {
         return CompletableFuture.completedFuture(builder.status(BAD_REQUEST).entity(Log.REST.illegalArgument("threads", threads).getMessage()).build());
      }

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);

      return CompletableFuture.supplyAsync(() -> {
         try {
            long hotrod = upgradeManager.synchronizeData("hotrod", readBatch, threads);
            builder.entity(Log.REST.synchronizedEntries(hotrod));
         } catch (Exception e) {
            Throwable rootCause = Util.getRootCause(e);
            builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(rootCause.getMessage());
         }
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> convertToJson(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String contents = restRequest.contents().asString();

      if (contents == null || contents.isEmpty()) {
         responseBuilder.status(HttpResponseStatus.BAD_REQUEST);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      ParserRegistry parserRegistry = invocationHelper.getParserRegistry();
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(contents);
      ConfigurationBuilder builder = builderHolder.getNamedConfigurationBuilders().values().iterator().next();
      Configuration configuration = builder.build();
      responseBuilder.contentType(APPLICATION_JSON).entity(invocationHelper.getJsonWriter().toJSON(configuration));
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> streamKeys(RestRequest request) {
      String cacheName = request.variables().get("cacheName");

      String batchParam = request.getParameter("batch");
      String limitParam = request.getParameter("limit");
      int batch = batchParam == null || batchParam.isEmpty() ? STREAM_BATCH_SIZE : Integer.parseInt(batchParam);
      int limit = limitParam == null || limitParam.isEmpty() ? -1 : Integer.parseInt(limitParam);

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, APPLICATION_JSON, APPLICATION_JSON, request);
      if (cache == null)
         return notFoundResponseFuture();

      // Streaming over the cache is blocking
      return CompletableFuture.supplyAsync(() -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         CacheStream<?> stream = cache.keySet().stream();
         if (limit > -1) {
            stream = stream.limit(limit);
         }
         responseBuilder.entity(new CacheKeyInputStream(stream, batch));

         responseBuilder.contentType(APPLICATION_JSON_TYPE);

         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> streamEntries(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      String limitParam = request.getParameter("limit");
      String metadataParam = request.getParameter("metadata");
      String batchParam = request.getParameter("batch");
      int limit = limitParam == null ? -1 : Integer.parseInt(limitParam);
      boolean metadata = metadataParam == null ? false : Boolean.parseBoolean(metadataParam);
      int batch = batchParam == null ? STREAM_BATCH_SIZE : Integer.parseInt(batchParam);

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, APPLICATION_JSON, APPLICATION_JSON, request);
      if (cache == null)
         return notFoundResponseFuture();

      // Streaming over the cache is blocking
      return CompletableFuture.supplyAsync(() -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         CacheStream<? extends Map.Entry<?, ?>> stream = cache.entrySet().stream();
         if (limit > -1) {
            stream = stream.limit(limit);
         }
         responseBuilder.entity(new CacheEntryInputStream(stream, batch, metadata));

         responseBuilder.contentType(APPLICATION_JSON_TYPE);

         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> removeCache(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return notFoundResponseFuture();

      return CompletableFuture.supplyAsync(() -> {
         restCacheManager.getCacheManagerAdmin(request).removeCache(cacheName);
         return new NettyRestResponse.Builder()
               .status(NO_CONTENT)
               .status(OK)
               .build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> cacheExists(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = restRequest.variables().get("cacheName");

      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND);
      }
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   private CompletableFuture<RestResponse> createCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder().status(NO_CONTENT);
      List<String> template = request.parameters().get("template");
      String cacheName = request.variables().get("cacheName");

      EnumSet<AdminFlag> adminFlags = request.getAdminFlags();
      EmbeddedCacheManagerAdmin initialAdmin = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
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
         try {
            administration.createCache(cacheName, finalCfgBuilder.build());
            responseBuilder.status(OK);
         } catch (Throwable t) {
            responseBuilder.status(BAD_REQUEST).entity(t.getMessage());
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getCacheStats(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      Stats stats = cache.getAdvancedCache().getStats();
      return asJsonResponseFuture(stats.toJson());
   }

   private CompletionStage<RestResponse> getAllDetails(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return notFoundResponseFuture();

      return CompletableFuture.supplyAsync(() -> getDetailResponse(cache), invocationHelper.getExecutor());
   }

   private RestResponse getDetailResponse(Cache<?, ?> cache) {
      Stats stats = cache.getAdvancedCache().getStats();
      Configuration configuration = cache.getCacheConfiguration();
      boolean statistics = configuration.statistics().enabled();
      int size = cache.getAdvancedCache().size();
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      SearchStatistics searchStatistics = Search.getSearchStatistics(cache);
      IndexStatistics indexStatistics = searchStatistics.getIndexStatistics();
      boolean rehashInProgress = distributionManager != null && distributionManager.isRehashInProgress();
      boolean indexingInProgress = indexStatistics.reindexing();
      boolean indexed = configuration.indexing().enabled();
      boolean queryable = invocationHelper.getRestCacheManager().isCacheQueryable(cache);

      CacheFullDetail fullDetail = new CacheFullDetail();
      fullDetail.stats = stats;
      fullDetail.configuration = invocationHelper.getJsonWriter().toJSON(configuration);
      fullDetail.size = size;
      fullDetail.rehashInProgress = rehashInProgress;
      fullDetail.indexingInProgress = indexingInProgress;
      fullDetail.persistent = configuration.persistence().usingStores();
      fullDetail.bounded = configuration.memory().evictionStrategy().isEnabled();
      fullDetail.indexed = indexed;
      fullDetail.hasRemoteBackup = configuration.sites().hasEnabledBackups();
      fullDetail.secured = configuration.security().authorization().enabled();
      fullDetail.transactional = configuration.transaction().transactionMode().isTransactional();
      fullDetail.statistics = statistics;
      fullDetail.queryable = queryable;

      return addEntityAsJson(fullDetail.toJson(), new NettyRestResponse.Builder()).build();
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
         return notFoundResponseFuture();

      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());

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

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      return cache.sizeAsync().thenApply(size -> asJsonResponse(Json.make(size)));
   }

   private CompletionStage<RestResponse> getCacheNames(RestRequest request) throws RestResponseException {
      Collection<String> cacheNames = invocationHelper.getRestCacheManager().getCacheNames();
      return asJsonResponseFuture(Json.make(cacheNames));
   }

   private static class CacheFullDetail implements JsonSerialization {
      public Stats stats;
      public int size;
      public String configuration;
      public boolean rehashInProgress;
      public boolean bounded;
      public boolean indexed;
      public boolean persistent;
      public boolean transactional;
      public boolean secured;
      public boolean hasRemoteBackup;
      public boolean indexingInProgress;
      public boolean statistics;
      public boolean queryable;

      @Override
      public Json toJson() {
         return Json.object()
               .set("stats", stats.toJson())
               .set("size", size)
               .set("configuration", Json.factory().raw(configuration))
               .set("rehash_in_progress", rehashInProgress)
               .set("bounded", bounded)
               .set("indexed", indexed)
               .set("persistent", persistent)
               .set("transactional", transactional)
               .set("secured", secured)
               .set("has_remote_backup", hasRemoteBackup)
               .set("indexing_in_progress", indexingInProgress)
               .set("statistics", statistics)
               .set("queryable", queryable);
      }
   }
}
