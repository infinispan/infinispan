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
import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;
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
import static org.infinispan.rest.resources.ResourceUtil.badRequestResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;
import static org.infinispan.rest.resources.ResourceUtil.notFoundResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.responseFuture;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration;
import org.infinispan.persistence.remote.upgrade.SerializationUtils;
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
import org.infinispan.rest.distribution.CacheDistributionInfo;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.tracing.RestTelemetryService;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.stats.Stats;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.upgrade.RollingUpgradeManager;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST resource to manage the caches.
 *
 * @since 10.0
 */
public class CacheResourceV2 extends BaseCacheResource implements ResourceHandler {

   private static final int STREAM_BATCH_SIZE = 1000;
   private static final String MIGRATOR_NAME = "hotrod";

   private final ParserRegistry parserRegistry = new ParserRegistry();

   public CacheResourceV2(InvocationHelper invocationHelper, RestTelemetryService telemetryService) {
      super(invocationHelper, telemetryService);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Key related operations
            .invocation().methods(PUT, POST).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::putValueToCache)
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::getCacheValue)
            .invocation().method(GET).path("/v2/caches/{cacheName}/{cacheKey}").withAction("distribution").handleWith(this::getKeyDistribution)
            .invocation().method(DELETE).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::deleteCacheValue)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("keys").handleWith(this::streamKeys)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("entries").handleWith(this::streamEntries)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("listen").handleWith(this::cacheListen)

            // Config and statistics
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}").withAction("config").handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("stats").handleWith(this::getCacheStats)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("distribution").handleWith(this::getCacheDistribution)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("get-mutable-attributes").permission(AuthorizationPermission.ADMIN).handleWith(this::getCacheConfigMutableAttributes)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("get-mutable-attribute").permission(AuthorizationPermission.ADMIN).handleWith(this::getCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("set-mutable-attribute").permission(AuthorizationPermission.ADMIN).handleWith(this::setCacheConfigMutableAttribute)

            // List
            .invocation().methods(GET).path("/v2/caches/").handleWith(this::getCacheNames)

            // Cache lifecycle
            .invocation().methods(POST, PUT).path("/v2/caches/{cacheName}").handleWith(this::createOrUpdate)
            .invocation().method(DELETE).path("/v2/caches/{cacheName}").handleWith(this::removeCache)
            .invocation().method(HEAD).path("/v2/caches/{cacheName}").handleWith(this::cacheExists)

            .invocation().method(GET).path("/v2/caches/{cacheName}").withAction("get-availability").permission(AuthorizationPermission.ADMIN).handleWith(this::getCacheAvailability)
            .invocation().method(POST).path("/v2/caches/{cacheName}").withAction("set-availability").permission(AuthorizationPermission.ADMIN).handleWith(this::setCacheAvailability)

            // Operations
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("clear").handleWith(this::clearEntireCache)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("size").handleWith(this::getSize)

            // Rolling Upgrade methods
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("sync-data").handleWith(this::syncData)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").deprecated().withAction("disconnect-source").handleWith(this::deleteSourceConnection)
            .invocation().methods(POST).path("/v2/caches/{cacheName}/rolling-upgrade/source-connection").handleWith(this::addSourceConnection)
            .invocation().methods(DELETE).path("/v2/caches/{cacheName}/rolling-upgrade/source-connection").handleWith(this::deleteSourceConnection)
            .invocation().methods(HEAD).path("/v2/caches/{cacheName}/rolling-upgrade/source-connection").handleWith(this::hasSourceConnections)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/rolling-upgrade/source-connection").handleWith(this::getSourceConnection)

            // Search
            .invocation().methods(GET, POST).path("/v2/caches/{cacheName}").withAction("search")
            .permission(AuthorizationPermission.BULK_READ)
            .handleWith(queryAction::search)

            // Misc
            .invocation().methods(POST).path("/v2/caches").withAction("toJSON").deprecated().handleWith(this::convertToJson)
            .invocation().methods(POST).path("/v2/caches").withAction("convert").handleWith(this::convert)

            // All details
            .invocation().methods(GET).path("/v2/caches/{cacheName}").handleWith(this::getAllDetails)

            // Enable Rebalance
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("enable-rebalancing")
            .permission(AuthorizationPermission.ADMIN).name("ENABLE REBALANCE").auditContext(AuditContext.CACHE)
            .handleWith(r -> setRebalancing(true, r))

            // Disable Rebalance
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("disable-rebalancing")
            .permission(AuthorizationPermission.ADMIN).name("DISABLE REBALANCE").auditContext(AuditContext.CACHE)
            .handleWith(r -> setRebalancing(false, r))

            .create();
   }

   @SuppressWarnings("rawtypes")
   private CompletionStage<RestResponse> getSourceConnection(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      PersistenceManager persistenceManager =
            SecurityActions.getPersistenceManager(invocationHelper.getRestCacheManager().getInstance(), cache.getName());

      List<RemoteStore> remoteStores = new ArrayList<>(persistenceManager.getStores(RemoteStore.class));

      if (remoteStores.isEmpty()) {
         builder.status(NOT_FOUND);
         return completedFuture(builder.build());
      }

      if (remoteStores.size() != 1) {
         builder.status(INTERNAL_SERVER_ERROR);
         builder.entity("More than one remote store detected, rolling upgrades aren't supported");
         return completedFuture(builder.build());
      }

      RemoteStoreConfiguration storeConfiguration = remoteStores.get(0).getConfiguration();

      builder.entity(SerializationUtils.toJson(storeConfiguration));
      return completedFuture(builder.build());
   }

   private CompletionStage<RestResponse> hasSourceConnections(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);

      return CompletableFuture.supplyAsync(() -> {
         try {
            if (!upgradeManager.isConnected(MIGRATOR_NAME)) {
               builder.status(NOT_FOUND);
            }
         } catch (Exception e) {
            builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(e.getMessage());
         }
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> deleteSourceConnection(RestRequest request) {
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      builder.status(NO_CONTENT);

      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
      try {
         if (upgradeManager.isConnected(MIGRATOR_NAME)) {
            upgradeManager.disconnectSource(MIGRATOR_NAME);
         } else {
            builder.status(HttpResponseStatus.NOT_MODIFIED);
         }
      } catch (Exception e) {
         builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(e.getMessage());
      }
      return completedFuture(builder.build());
   }

   private CompletionStage<RestResponse> addSourceConnection(RestRequest request) {
      final NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      builder.status(NO_CONTENT);

      String cacheName = request.variables().get("cacheName");
      ContentSource contents = request.contents();
      byte[] config = contents.rawContent();

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      if (cache == null) {
         return notFoundResponseFuture();
      }

      if (config == null || config.length == 0) {
         return badRequestResponseFuture("A remote-store config must be provided");
      }

      String storeConfig = new String(config, UTF_8);
      Json read = Json.read(storeConfig);

      if (!read.isObject() || read.at("remote-store") == null || read.asMap().size() != 1) {
         return badRequestResponseFuture("Invalid remote-store JSON description: a single remote-store element must be provided");
      }

      return CompletableFuture.supplyAsync(() -> {
         RollingUpgradeManager upgradeManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RollingUpgradeManager.class);
         try {
            RemoteStoreConfiguration storeConfiguration = SerializationUtils.fromJson(read.toString());
            if (!upgradeManager.isConnected(MIGRATOR_NAME)) {
               upgradeManager.connectSource(MIGRATOR_NAME, storeConfiguration);
            } else {
               builder.status(HttpResponseStatus.NOT_MODIFIED);
            }
         } catch (Exception e) {
            Throwable rootCause = Util.getRootCause(e);
            builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(rootCause.getMessage());
         }
         return builder.build();
      }, invocationHelper.getExecutor());
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
            long hotrod = upgradeManager.synchronizeData(MIGRATOR_NAME, readBatch, threads);
            builder.entity(Log.REST.synchronizedEntries(hotrod));
         } catch (Exception e) {
            Throwable rootCause = Util.getRootCause(e);
            builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(rootCause.getMessage());
         }
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> convert(RestRequest request, MediaType toType) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));
      String contents = request.contents().asString();

      if (contents == null || contents.isEmpty()) {
         responseBuilder.status(HttpResponseStatus.BAD_REQUEST);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      return CompletableFuture.supplyAsync(() -> {
         ParserRegistry parserRegistry = invocationHelper.getParserRegistry();
         ConfigurationBuilderHolder builderHolder = parserRegistry.parse(contents, request.contentType());
         Map.Entry<String, ConfigurationBuilder> entry = builderHolder.getNamedConfigurationBuilders().entrySet().iterator().next();
         Configuration configuration = entry.getValue().build();
         StringBuilderWriter out = new StringBuilderWriter();
         try (ConfigurationWriter writer = ConfigurationWriter.to(out).withType(toType).clearTextSecrets(true).prettyPrint(pretty).build()) {
            parserRegistry.serialize(writer, entry.getKey(), configuration);
         }
         return responseBuilder.contentType(toType).entity(out.toString()).build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> convertToJson(RestRequest request) {
      return convert(request, APPLICATION_JSON);
   }

   private CompletionStage<RestResponse> convert(RestRequest request) {
      return convert(request, negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML));
   }

   private CompletionStage<RestResponse> streamKeys(RestRequest request) {
      String cacheName = request.variables().get("cacheName");

      String batchParam = request.getParameter("batch");
      String limitParam = request.getParameter("limit");
      int batch = batchParam == null || batchParam.isEmpty() ? STREAM_BATCH_SIZE : Integer.parseInt(batchParam);
      int limit = limitParam == null || limitParam.isEmpty() ? -1 : Integer.parseInt(limitParam);

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, TEXT_PLAIN, MATCH_ALL, request);
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

      final MediaType keyMediaType = getMediaType(negotiate, cache, true);
      final MediaType valueMediaType = getMediaType(negotiate, cache, false);

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

   private MediaType getMediaType(boolean negotiate, AdvancedCache<?, ?> cache, boolean forKey) {
      MediaType storageMediaType = (forKey) ?
            cache.getKeyDataConversion().getStorageMediaType() :
            cache.getValueDataConversion().getStorageMediaType();

      boolean protoStreamEncoding = MediaType.APPLICATION_PROTOSTREAM.equals(storageMediaType);
      if (negotiate) {
         return negotiateEntryMediaType(storageMediaType, protoStreamEncoding);
      }
      return (protoStreamEncoding) ? APPLICATION_JSON : TEXT_PLAIN;
   }

   private MediaType negotiateEntryMediaType(MediaType storage, boolean protoStreamEncoding) {
      EncoderRegistry encoderRegistry = invocationHelper.getEncoderRegistry();
      boolean encodingDefined = !MediaType.APPLICATION_UNKNOWN.equals(storage);
      boolean jsonSupported = encodingDefined && encoderRegistry.isConversionSupported(storage, APPLICATION_JSON);
      boolean textSupported = encodingDefined && encoderRegistry.isConversionSupported(storage, TEXT_PLAIN);

      if (protoStreamEncoding) {
         if (jsonSupported) return APPLICATION_JSON;
         if (textSupported) return TEXT_PLAIN;
      } else {
         if (textSupported) return TEXT_PLAIN;
         if (jsonSupported) return APPLICATION_JSON;
      }

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

   private CompletableFuture<RestResponse> createOrUpdate(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      List<String> template = request.parameters().get("template");
      String cacheName = request.variables().get("cacheName");

      EnumSet<AdminFlag> adminFlags = request.getAdminFlags();
      if (request.method() == PUT) {
         if (adminFlags == null) {
            adminFlags = EnumSet.of(AdminFlag.UPDATE);
         } else {
            adminFlags.add(AdminFlag.UPDATE);
         }
      }
      EmbeddedCacheManagerAdmin initialAdmin = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      EmbeddedCacheManagerAdmin administration = adminFlags == null ? initialAdmin : initialAdmin.withFlags(adminFlags);

      if (template != null && !template.isEmpty()) {
         if (request.method() == PUT) {
            return CompletableFuture.completedFuture(responseBuilder.status(BAD_REQUEST).build());
         }
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
         if (request.method() == PUT) {
            return CompletableFuture.completedFuture(responseBuilder.status(BAD_REQUEST).build());
         }
         return CompletableFuture.supplyAsync(() -> {
            administration.createCache(cacheName, (String) null);
            responseBuilder.status(OK);
            return responseBuilder.build();
         }, invocationHelper.getExecutor());
      }
      MediaType sourceType = request.contentType() == null ? APPLICATION_JSON : request.contentType();
      if (!sourceType.match(APPLICATION_JSON) && !sourceType.match(APPLICATION_XML) && !sourceType.match(APPLICATION_YAML)) {
         responseBuilder.status(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(invocationHelper.getRestCacheManager().getInstance());
      return CompletableFuture.supplyAsync(() -> {
         try {
            ConfigurationBuilderHolder holder = invocationHelper.getParserRegistry().parse(new String(bytes, UTF_8), sourceType);
            ConfigurationBuilder cfgBuilder = holder.getCurrentConfigurationBuilder() != null ? holder.getCurrentConfigurationBuilder() : new ConfigurationBuilder();
            if (request.method() == PUT) {
               administration.getOrCreateCache(cacheName, cfgBuilder.build(globalConfiguration));
            } else {
               administration.createCache(cacheName, cfgBuilder.build(globalConfiguration));
            }
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
            asJsonResponse(cache.getAdvancedCache().getStats().toJson(), isPretty(request)), invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getCacheDistribution(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<?> cache = invocationHelper.getRestCacheManager();
      boolean pretty = isPretty(request);
      return CompletableFuture.supplyAsync(() -> cache.cacheDistribution(cacheName, request), invocationHelper.getExecutor())
            .thenCompose(Function.identity())
            .thenApply(distributions -> asJsonResponse(Json.array(distributions.stream().map(CacheDistributionInfo::toJson).toArray()), pretty));
   }

   private CompletionStage<RestResponse> getKeyDistribution(RestRequest request) {
      boolean pretty = isPretty(request);
      return keyDistribution(request)
            .thenApply(distribution -> asJsonResponse(distribution.toJson(), pretty));
   }

   private CompletionStage<RestResponse> getAllDetails(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      boolean pretty = isPretty(request);
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return notFoundResponseFuture();

      return CompletableFuture.supplyAsync(() -> getDetailResponse(cache, pretty), invocationHelper.getExecutor());
   }

   private RestResponse getDetailResponse(Cache<?, ?> cache, boolean pretty) {
      Configuration configuration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      PersistenceManager persistenceManager = SecurityActions.getPersistenceManager(cacheManager, cache.getName());
      Stats stats = null;
      Boolean rehashInProgress = null;
      Boolean indexingInProgress = null;
      Boolean queryable = null;

      try {
         // TODO Shouldn't we return the clustered stats, like Hot Rod does?
         stats = cache.getAdvancedCache().getStats();
         DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
         rehashInProgress = distributionManager != null && distributionManager.isRehashInProgress();
      } catch (SecurityException ex) {
         // Admin is needed
      }

      Boolean rebalancingEnabled = null;
      try {
         LocalTopologyManager localTopologyManager = SecurityActions.getComponentRegistry(cache.getAdvancedCache())
               .getComponent(LocalTopologyManager.class);
         if (localTopologyManager != null) {
            rebalancingEnabled = localTopologyManager.isCacheRebalancingEnabled(cache.getName());
         }
      } catch (Exception ex) {
         // Getting rebalancing status might raise an exception
      }

      Integer size = null;
      if (globalConfiguration.metrics().accurateSize()) {
         try {
            size = cache.size();
         } catch (SecurityException ex) {
            // Bulk Read is needed
         }
      }

      SearchStatistics searchStatistics = Search.getSearchStatistics(cache);
      IndexStatistics indexStatistics = searchStatistics.getIndexStatistics();
      indexingInProgress = indexStatistics.reindexing();
      queryable = invocationHelper.getRestCacheManager().isCacheQueryable(cache);

      boolean statistics = configuration.statistics().enabled();
      boolean indexed = configuration.indexing().enabled();

      CacheFullDetail fullDetail = new CacheFullDetail();
      fullDetail.stats = stats;
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(pretty).build()) {
         invocationHelper.getParserRegistry().serialize(w, cache.getName(), configuration);
      }
      fullDetail.configuration = sw.toString();
      fullDetail.size = size;
      fullDetail.rehashInProgress = rehashInProgress;
      fullDetail.indexingInProgress = indexingInProgress;
      fullDetail.persistent = persistenceManager.isEnabled();
      fullDetail.bounded = configuration.memory().whenFull().isEnabled();
      fullDetail.indexed = indexed;
      fullDetail.hasRemoteBackup = configuration.sites().hasBackups();
      fullDetail.secured = configuration.security().authorization().enabled();
      fullDetail.transactional = configuration.transaction().transactionMode().isTransactional();
      fullDetail.statistics = statistics;
      fullDetail.queryable = queryable;
      fullDetail.rebalancingEnabled = rebalancingEnabled;
      fullDetail.keyStorage = cache.getAdvancedCache().getKeyDataConversion().getStorageMediaType();
      fullDetail.valueStorage = cache.getAdvancedCache().getValueDataConversion().getStorageMediaType();

      return addEntityAsJson(fullDetail.toJson(), new NettyRestResponse.Builder(), pretty).build();
   }

   private CompletionStage<RestResponse> getCacheConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));

      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML);
      responseBuilder.contentType(accept);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
      }
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return notFoundResponseFuture();

      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());

      ByteArrayOutputStream entity = new ByteArrayOutputStream();
      try (ConfigurationWriter writer = ConfigurationWriter.to(entity).withType(accept).prettyPrint(pretty).build()) {
         parserRegistry.serialize(writer, cacheName, cacheConfiguration);
      } catch (Exception e) {
         return CompletableFuture.completedFuture(responseBuilder.status(INTERNAL_SERVER_ERROR).entity(Util.getRootCause(e)).build());
      }
      responseBuilder.entity(entity);
      return CompletableFuture.completedFuture(responseBuilder.status(OK).build());
   }

   private CompletionStage<RestResponse> getCacheAvailability(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      // Use EmbeddedCacheManager directly to allow internal caches to be updated
      if (!invocationHelper.getRestCacheManager().getInstance().isRunning(cacheName))
         return notFoundResponseFuture();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getInstance().getCache(cacheName).getAdvancedCache();
      if (cache == null) {
         return notFoundResponseFuture();
      }
      AvailabilityMode availability = cache.getAvailability();
      return CompletableFuture.completedFuture(
            new NettyRestResponse.Builder()
                  .entity(availability)
                  .contentType(TEXT_PLAIN)
                  .status(OK)
                  .build()
      );
   }

   private CompletionStage<RestResponse> setCacheAvailability(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      String availability = request.getParameter("availability");
      // Use EmbeddedCacheManager directly to allow internal caches to be updated
      if (!invocationHelper.getRestCacheManager().getInstance().isRunning(cacheName))
         return notFoundResponseFuture();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getInstance().getCache(cacheName).getAdvancedCache();
      if (cache == null) {
         return notFoundResponseFuture();
      }
      try {
         AvailabilityMode availabilityMode = AvailabilityMode.valueOf(availability.toUpperCase());
         cache.setAvailability(availabilityMode);
         return CompletableFuture.completedFuture(new NettyRestResponse.Builder().status(NO_CONTENT).build());
      } catch (IllegalArgumentException e) {
         return badRequestResponseFuture(String.format("Unknown AvailabilityMode '%s'", availability));
      }
   }

   private CompletionStage<RestResponse> getCacheConfigMutableAttributes(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String cacheName = request.variables().get("cacheName");
      boolean full = Boolean.parseBoolean(request.getParameter("full"));

      responseBuilder.contentType(APPLICATION_JSON);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
      }
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return notFoundResponseFuture();

      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      Map<String, Attribute> attributes = new LinkedHashMap<>();
      mutableAttributes(cacheConfiguration, attributes, null);
      if (full) {
         Json all = Json.object();
         for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            Attribute attribute = entry.getValue();
            Class<?> type = attribute.getAttributeDefinition().getType();
            Json object = Json.object("value", attribute.get(), "type", type.getSimpleName().toLowerCase());
            if (type.isEnum()) {
               object.set("universe", Arrays.stream(type.getEnumConstants()).map(Object::toString).collect(Collectors.toList()));
            }
            all.set(entry.getKey(), object);
         }
         return asJsonResponseFuture(all, isPretty(request));
      } else {
         return asJsonResponseFuture(Json.make(attributes.keySet()), isPretty(request));
      }
   }

   private static void mutableAttributes(ConfigurationElement<?> element, Map<String, Attribute> attributes, String prefix) {
      prefix = prefix == null ? "" : element.elementName();
      for (Attribute<?> attribute : element.attributes().attributes()) {
         if (!attribute.isImmutable()) {
            attributes.put(prefix + "." + attribute.getAttributeDefinition().name(), attribute);
         }
      }
      for (ConfigurationElement<?> child : element.children()) {
         mutableAttributes(child, attributes, prefix);
      }
   }

   private CompletionStage<RestResponse> getCacheConfigMutableAttribute(RestRequest request) {
      String attributeName = request.getParameter("attribute-name");
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         return notFoundResponseFuture();
      }
      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      Attribute<?> attribute = cacheConfiguration.findAttribute(attributeName);
      if (attribute.isImmutable()) {
         return responseFuture(BAD_REQUEST);
      } else {
         return asJsonResponseFuture(Json.make(String.valueOf(attribute.get())), isPretty(request));
      }
   }

   private CompletionStage<RestResponse> setCacheConfigMutableAttribute(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String attributeName = request.getParameter("attribute-name");
      String attributeValue = request.getParameter("attribute-value");
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         return notFoundResponseFuture();
      }
      Configuration configuration = new ConfigurationBuilder().read(SecurityActions.getCacheConfiguration(cache.getAdvancedCache())).build();
      Attribute<?> attribute = configuration.findAttribute(attributeName);
      invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      EmbeddedCacheManagerAdmin administration = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request).withFlags(AdminFlag.UPDATE);
      return CompletableFuture.supplyAsync(() -> {
         try {
            attribute.fromString(attributeValue);
            administration.getOrCreateCache(cacheName, configuration);
            responseBuilder.status(OK);
         } catch (Throwable t) {
            responseBuilder.status(BAD_REQUEST).entity(Util.getRootCause(t).getMessage());
         }
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getSize(RestRequest request) {
      String cacheName = request.variables().get("cacheName");

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      boolean pretty = isPretty(request);
      return cache.sizeAsync().thenApply(size -> asJsonResponse(Json.make(size), pretty));
   }

   private CompletionStage<RestResponse> getCacheNames(RestRequest request) throws RestResponseException {
      Collection<String> cacheNames = invocationHelper.getRestCacheManager().getCacheNames();
      return asJsonResponseFuture(Json.make(cacheNames), isPretty(request));
   }

   private CompletionStage<RestResponse> setRebalancing(boolean enable, RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return notFoundResponseFuture();

      return CompletableFuture.supplyAsync(() -> {
         NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
         LocalTopologyManager ltm = SecurityActions.getGlobalComponentRegistry(restCacheManager.getInstance()).getLocalTopologyManager();
         try {
            ltm.setCacheRebalancingEnabled(cacheName, enable);
            builder.status(NO_CONTENT);
         } catch (Exception e) {
            builder.status(INTERNAL_SERVER_ERROR).entity(e.getMessage());
         }
         return builder.build();
      }, invocationHelper.getExecutor());
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
      public Boolean rebalancingEnabled;
      public MediaType keyStorage;
      public MediaType valueStorage;

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

         if (rebalancingEnabled != null) {
            json.set("rebalancing_enabled", rebalancingEnabled);
         }

         return json
               .set("configuration", Json.factory().raw(configuration))
               .set("bounded", bounded)
               .set("indexed", indexed)
               .set("persistent", persistent)
               .set("transactional", transactional)
               .set("secured", secured)
               .set("has_remote_backup", hasRemoteBackup)
               .set("statistics", statistics)
               .set("key_storage", keyStorage)
               .set("value_storage", valueStorage);
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
