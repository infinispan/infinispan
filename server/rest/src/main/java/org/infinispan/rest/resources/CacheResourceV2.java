package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
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

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
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
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.rest.CacheEntryInputStream;
import org.infinispan.rest.CacheKeyInputStream;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.ResponseHeader;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.ServerSentEvent;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.security.AuthorizationPermission;
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
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("listen").handleWith(this::cacheListen)

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
            .invocation().methods(GET, POST).path("/v2/caches/{cacheName}").withAction("search")
            .permission(AuthorizationPermission.BULK_READ)
            .handleWith(queryAction::search)

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
      String negotiateMediaType = request.getParameter("content-negotiation");

      int limit = limitParam == null ? -1 : Integer.parseInt(limitParam);
      boolean metadata = Boolean.parseBoolean(metadataParam);
      int batch = batchParam == null ? STREAM_BATCH_SIZE : Integer.parseInt(batchParam);
      boolean negotiate = Boolean.parseBoolean(negotiateMediaType);

      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request).getAdvancedCache();
      if (cache == null) return notFoundResponseFuture();

      final MediaType keyMediaType = negotiate ? negotiateEntryMediaType(cache, true) : APPLICATION_JSON;
      final MediaType valueMediaType = negotiate ? negotiateEntryMediaType(cache, false) : APPLICATION_JSON;

      Cache<?, ?> streamCache = invocationHelper.getRestCacheManager().getCache(cacheName, keyMediaType, valueMediaType, request);

      // Streaming over the cache is blocking
      return CompletableFuture.supplyAsync(() -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         CacheStream<? extends Map.Entry<?, ?>> stream = streamCache.entrySet().stream();
         if (limit > -1) {
            stream = stream.limit(limit);
         }
         responseBuilder.entity(new CacheEntryInputStream(keyMediaType.match(APPLICATION_JSON), valueMediaType.match(APPLICATION_JSON), stream, batch, metadata));

         responseBuilder.contentType(APPLICATION_JSON_TYPE);
         responseBuilder.header(ResponseHeader.KEY_CONTENT_TYPE_HEADER.getValue(), keyMediaType.toString());
         responseBuilder.header(ResponseHeader.VALUE_CONTENT_TYPE_HEADER.getValue(), valueMediaType.toString());

         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> cacheListen(RestRequest request) {
      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, TEXT_PLAIN);
      String cacheName = request.variables().get("cacheName");
      boolean includeCurrentState = Boolean.parseBoolean(request.getParameter("includeCurrentState"));
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return notFoundResponseFuture();
      Cache<?, ?> cache = restCacheManager.getCache(cacheName, accept, accept, request);
      BaseCacheListener listener = includeCurrentState ? new StatefulCacheListener(cache) : new StatelessCacheListener(cache);
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      responseBuilder.contentType(TEXT_EVENT_STREAM).entity(listener.getEventStream());
      return cache.addListenerAsync(listener).thenApply(v -> responseBuilder.build());
   }

   private MediaType negotiateEntryMediaType(AdvancedCache<?, ?> cache, boolean forKey) {
      MediaType storage = forKey ? cache.getKeyDataConversion().getStorageMediaType() : cache.getValueDataConversion().getStorageMediaType();
      EncoderRegistry encoderRegistry = invocationHelper.getEncoderRegistry();
      boolean encodingDefined = !MediaType.APPLICATION_UNKNOWN.equals(storage);
      boolean jsonSupported = encodingDefined && encoderRegistry.isConversionSupported(storage, APPLICATION_JSON);
      boolean textSupported = encodingDefined && encoderRegistry.isConversionSupported(storage, TEXT_PLAIN);

      if (jsonSupported) return APPLICATION_JSON;

      if (textSupported) return TEXT_PLAIN;

      if (encodingDefined) return storage.withEncoding("hex");

      return APPLICATION_OCTET_STREAM.withEncoding("hex");
   }

   private CompletionStage<RestResponse> removeCache(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return notFoundResponseFuture();

      return CompletableFuture.supplyAsync(() -> {
         restCacheManager.getCacheManagerAdmin(request).removeCache(cacheName);
         return new NettyRestResponse.Builder()
               .status(OK)
               .build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> cacheExists(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = restRequest.variables().get("cacheName");

      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND);
      } else {
         responseBuilder.status(NO_CONTENT);
      }
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   private CompletableFuture<RestResponse> createCache(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
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
      return CompletableFuture.supplyAsync(() ->
            asJsonResponse(cache.getAdvancedCache().getStats().toJson()), invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getAllDetails(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return notFoundResponseFuture();

      return CompletableFuture.supplyAsync(() -> getDetailResponse(cache), invocationHelper.getExecutor());
   }

   private RestResponse getDetailResponse(Cache<?, ?> cache) {
      Configuration configuration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      Stats stats = null;
      Boolean rehashInProgress = null;
      Boolean indexingInProgress = null;
      Boolean queryable = null;
      try {
         stats = cache.getAdvancedCache().getStats();
         DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
         rehashInProgress = distributionManager != null && distributionManager.isRehashInProgress();
      } catch (SecurityException ex) {
         // Admin is needed
      }

      Integer size = null;
      try {
         size = cache.size();
      } catch (SecurityException ex) {
         // Bulk Read is needed
      }

      SearchStatistics searchStatistics = Search.getSearchStatistics(cache);
      IndexStatistics indexStatistics = searchStatistics.getIndexStatistics();
      indexingInProgress = indexStatistics.reindexing();
      queryable = invocationHelper.getRestCacheManager().isCacheQueryable(cache);

      boolean statistics = configuration.statistics().enabled();
      boolean indexed = configuration.indexing().enabled();

      CacheFullDetail fullDetail = new CacheFullDetail();
      fullDetail.stats = stats;
      fullDetail.configuration = invocationHelper.getJsonWriter().toJSON(configuration);
      fullDetail.size = size;
      fullDetail.rehashInProgress = rehashInProgress;
      fullDetail.indexingInProgress = indexingInProgress;
      fullDetail.persistent = configuration.persistence().usingStores();
      fullDetail.bounded = configuration.memory().whenFull().isEnabled();
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

      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML);
      responseBuilder.contentType(accept);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
      }
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return notFoundResponseFuture();

      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      ParserRegistry registry = new ParserRegistry();

      switch (accept.getTypeSubtype()) {
         case APPLICATION_JSON_TYPE:
            responseBuilder.entity(invocationHelper.getJsonWriter().toJSON(cacheConfiguration));
            break;
         default:
            ByteArrayOutputStream entity = new ByteArrayOutputStream();
            try (ConfigurationWriter writer = ConfigurationWriter.to(entity).withType(accept).build()) {
               registry.serialize(writer, null, Collections.singletonMap(cacheName, cacheConfiguration));
            } catch (Exception e) {
               return CompletableFuture.completedFuture(responseBuilder.status(INTERNAL_SERVER_ERROR).entity(Util.getRootCause(e)).build());
            }
            responseBuilder.entity(entity);
      }
      return CompletableFuture.completedFuture(responseBuilder.status(OK).build());
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
      public Integer size;
      public String configuration;
      public Boolean rehashInProgress;
      public boolean bounded;
      public boolean indexed;
      public boolean persistent;
      public boolean transactional;
      public boolean secured;
      public boolean hasRemoteBackup;
      public Boolean indexingInProgress;
      public boolean statistics;
      public Boolean queryable;

      @Override
      public Json toJson() {
         Json json = Json.object();

         if (stats != null) {
            json.set("stats", stats.toJson());
         }

         if (size != null) {
            json.set("size", size);
         }

         if (rehashInProgress != null) {
            json.set("rehash_in_progress", rehashInProgress);
         }

         if (indexingInProgress != null) {
            json.set("indexing_in_progress", indexingInProgress);
         }

         if (queryable != null) {
            json.set("queryable", queryable);
         }

         return json
               .set("configuration", Json.factory().raw(configuration))
               .set("bounded", bounded)
               .set("indexed", indexed)
               .set("persistent", persistent)
               .set("transactional", transactional)
               .set("secured", secured)
               .set("has_remote_backup", hasRemoteBackup)
               .set("statistics", statistics);
      }
   }

   public static abstract class BaseCacheListener {
      protected final Cache<?, ?> cache;
      protected final EventStream eventStream;

      protected BaseCacheListener(Cache<?, ?> cache) {
         this.cache = cache;
         this.eventStream = new EventStream(null, () -> cache.removeListenerAsync(this));
      }

      public EventStream getEventStream() {
         return eventStream;
      }

      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      @CacheEntryExpired
      public CompletionStage<Void> onCacheEvent(CacheEntryEvent<?, ?> event) {
         ServerSentEvent sse = new ServerSentEvent(event.getType().name().toLowerCase().replace('_', '-'), new String((byte[]) event.getKey()));
         return eventStream.sendEvent(sse);
      }
   }

   @Listener(clustered = true, includeCurrentState = true)
   public static class StatefulCacheListener extends BaseCacheListener {
      public StatefulCacheListener(Cache<?, ?> cache) {
         super(cache);
      }
   }

   @Listener(clustered = true)
   public static class StatelessCacheListener extends BaseCacheListener {

      public StatelessCacheListener(Cache<?, ?> cache) {
         super(cache);
      }
   }
}
