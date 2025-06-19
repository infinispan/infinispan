package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
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
import static org.infinispan.commons.util.EnumUtil.EMPTY_BIT_SET;
import static org.infinispan.commons.util.Util.unwrapExceptionMessage;
import static org.infinispan.rest.RestRequestHandler.filterCause;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.EncoderEntryMapper;
import org.infinispan.cache.impl.EncoderKeyMapper;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SecurityConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
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
import org.infinispan.query.core.stats.impl.SearchStatsRetriever;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.reactive.publisher.PublisherTransformers;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestRequest;
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
import org.infinispan.rest.stream.CacheChunkedStream;
import org.infinispan.rest.stream.CacheEntryStreamProcessor;
import org.infinispan.rest.stream.CacheKeyStreamProcessor;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Role;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.upgrade.RollingUpgradeManager;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostMultipartRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MemoryAttribute;
import io.reactivex.rxjava3.core.Flowable;

/**
 * REST resource to manage the caches.
 *
 * @since 10.0
 */
public class CacheResourceV2 extends BaseCacheResource implements ResourceHandler {

   @Deprecated
   private static final Map<String, Configuration> SERVER_TEMPLATES = Map.of(
         "org.infinispan.LOCAL", createServerTemplate(CacheMode.LOCAL),
         "org.infinispan.REPL_SYNC", createServerTemplate(CacheMode.REPL_SYNC),
         "org.infinispan.REPL_ASYNC", createServerTemplate(CacheMode.REPL_ASYNC),
         "org.infinispan.DIST_SYNC", createServerTemplate(CacheMode.DIST_SYNC),
         "org.infinispan.DIST_ASYNC", createServerTemplate(CacheMode.DIST_ASYNC),
         "org.infinispan.INVALIDATION_SYNC", createServerTemplate(CacheMode.INVALIDATION_SYNC),
         "org.infinispan.INVALIDATION_ASYNC", createServerTemplate(CacheMode.INVALIDATION_ASYNC)

   );

   protected static final int STREAM_BATCH_SIZE = 1000;
   protected static final String MIGRATOR_NAME = "hotrod";

   protected final ParserRegistry parserRegistry = new ParserRegistry();
   protected final InternalCacheRegistry internalCacheRegistry;
   protected final ServerStateManager serverStateManager;

   public CacheResourceV2(InvocationHelper invocationHelper, InfinispanTelemetry telemetryService) {
      super(invocationHelper, telemetryService);
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
      this.serverStateManager = globalComponentRegistry.getComponent(ServerStateManager.class);
      this.internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
   }

   /**
    * @deprecated to be removed after DefaultTemplate enum
    */
   @Deprecated(forRemoval = true)
   private static Configuration createServerTemplate(CacheMode cacheMode) {
      var builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode);
      builder.statistics().enable();
      return builder.build();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("cache", "REST resource to manage caches.")
            // Key related operations
            .invocation().methods(PUT, POST).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::putValueToCache)
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::getCacheValue)
            .invocation().method(GET).path("/v2/caches/{cacheName}/{cacheKey}").withAction("distribution").handleWith(this::getKeyDistribution)
            .invocation().method(GET).path("/v2/caches/{cacheName}/{cacheKey}").withAction("distribution").handleWith(this::getKeyDistribution)
            .invocation().method(DELETE).path("/v2/caches/{cacheName}/{cacheKey}").handleWith(this::deleteCacheValue)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("keys").handleWith(this::streamKeys)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("entries").handleWith(this::streamEntries)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("listen").handleWith(this::cacheListen)

            // Config
            .invocation().methods(GET, HEAD).path("/v2/caches/{cacheName}").withAction("config").permission(AuthorizationPermission.ADMIN).handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("get-mutable-attributes").permission(AuthorizationPermission.ADMIN).handleWith(this::getCacheConfigMutableAttributes)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("get-mutable-attribute").permission(AuthorizationPermission.ADMIN).handleWith(this::getCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("set-mutable-attribute").permission(AuthorizationPermission.ADMIN).handleWith(this::setCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("assign-alias").permission(AuthorizationPermission.ADMIN).handleWith(this::assignAlias)

              // Stats
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("stats").handleWith(this::getCacheStats)
            .invocation().methods(POST).path("/v2/caches/{cacheName}").withAction("stats-reset").permission(AuthorizationPermission.ADMIN).handleWith(this::resetCacheStats)
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("distribution").handleWith(this::getCacheDistribution)

            // List
            .invocation().methods(GET).path("/v2/caches/").handleWith(this::getCacheNames)
            .invocation().methods(GET).deprecated().path("/v2/cache-managers/{name}/caches").handleWith(this::getCaches)
            .invocation().methods(GET).path("/v2/caches").withAction("detailed").handleWith(this::getCaches)


            // List of caches for role
            .invocation().methods(GET).path("/v2/caches").withAction("role-accessible")
            .permission(AuthorizationPermission.ADMIN).name("CACHES PER ROLE LIST").auditContext(AuditContext.CACHE)
            .handleWith(this::getCacheNamesPerRole)

            // Health
            .invocation().methods(GET).path("/v2/caches/{cacheName}").withAction("health").handleWith(this::getCacheHealth)

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

            // Search - Delete By Query
            .invocation().methods(DELETE, POST).path("/v2/caches/{cacheName}").withAction("deleteByQuery")
            .permission(AuthorizationPermission.BULK_WRITE)
            .handleWith(queryAction::deleteByQuery)

            // Misc
            .invocation().methods(POST).path("/v2/caches").withAction("toJSON").deprecated().handleWith(this::convertToJson)
            .invocation().methods(POST).path("/v2/caches").withAction("convert").handleWith(this::convert)
            .invocation().methods(POST).path("/v2/caches").withAction("compare").handleWith(this::compare)

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

            // Restore after a shutdown
            .invocation().method(POST).path("/v2/caches/{cacheName}").withAction("initialize")
            .permission(AuthorizationPermission.ADMIN).name("Initialize cache").auditContext(AuditContext.CACHE)
            .handleWith(this::reinitializeCache)

            .create();
   }

   private CompletionStage<RestResponse> getCaches(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      if (responseBuilder.getHttpStatus() == NOT_FOUND) return completedFuture(responseBuilder.build());

      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());
      // Remove internal caches
      Set<String> cacheNames = new HashSet<>(subjectCacheManager.getAccessibleCacheNames());
      cacheNames.removeAll(internalCacheRegistry.getInternalCacheNames());
      cacheNames.removeIf(n -> !cacheManager.cacheExists(n));


      Set<String> ignoredCaches = serverStateManager.getIgnoredCaches();
      List<CacheHealth> cachesHealth = SecurityActions.getHealth(subjectCacheManager).getCacheHealth(cacheNames);

      LocalTopologyManager localTopologyManager = null;
      Boolean clusterRebalancingEnabled = null;
      try {
         localTopologyManager = SecurityActions.getGlobalComponentRegistry(cacheManager).getLocalTopologyManager();
         if (localTopologyManager != null) {
            clusterRebalancingEnabled = localTopologyManager.isRebalancingEnabled();
         }
      } catch (Exception e) {
         // Unable to get the component. Just ignore.
      }

      // We rely on the fact that getCacheNames doesn't block for embedded - remote it does unfortunately
      LocalTopologyManager finalLocalTopologyManager = localTopologyManager;
      Boolean finalClusterRebalancingEnabled = clusterRebalancingEnabled;
      boolean pretty = isPretty(request);
      return Flowable.fromIterable(cachesHealth)
            .map(chHealth -> getCacheInfo(request, cacheManager, subjectCacheManager, ignoredCaches,
                  finalLocalTopologyManager, finalClusterRebalancingEnabled, chHealth))
            .sorted(Comparator.comparing(c -> c.name))
            .collect(Collectors.toList()).map(cacheInfos -> (RestResponse) addEntityAsJson(Json.make(cacheInfos), responseBuilder, pretty).build())
            .toCompletionStage();
   }

   private CacheInfo getCacheInfo(RestRequest request, EmbeddedCacheManager cacheManager,
                                                    EmbeddedCacheManager subjectCacheManager, Set<String> ignoredCaches,
                                                    LocalTopologyManager finalLocalTopologyManager,
                                                    Boolean finalClusterRebalancingEnabled, CacheHealth chHealth) {
      CacheInfo cacheInfo = new CacheInfo();
      String cacheName = chHealth.getCacheName();
      cacheInfo.name = cacheName;
      HealthStatus cacheHealth = chHealth.getStatus();
      cacheInfo.health = cacheHealth;
      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(subjectCacheManager, cacheName);
      cacheInfo.type = cacheConfiguration.clustering().cacheMode().toCacheType();
      boolean isPersistent = false;
      if (chHealth.getStatus() != HealthStatus.FAILED) {
         PersistenceManager pm = SecurityActions.getCacheComponent(cacheManager, cacheName, PersistenceManager.class);
         isPersistent = pm.isEnabled();
      }
      cacheInfo.simpleCache = cacheConfiguration.simpleCache();
      cacheInfo.transactional = cacheConfiguration.transaction().transactionMode().isTransactional();
      cacheInfo.persistent = isPersistent;
      cacheInfo.persistent = cacheConfiguration.persistence().usingStores();
      cacheInfo.bounded = cacheConfiguration.memory().whenFull().isEnabled();
      cacheInfo.secured = cacheConfiguration.security().authorization().enabled();
      cacheInfo.indexed = cacheConfiguration.indexing().enabled();
      cacheInfo.hasRemoteBackup = cacheConfiguration.sites().hasBackups();
      cacheInfo.tracing = cacheConfiguration.tracing().enabled();
      cacheInfo.aliases = cacheConfiguration.aliases();

      // If the cache is ignored, status is IGNORED
      if (ignoredCaches.contains(cacheName)) {
         cacheInfo.status = "IGNORED";
      } else {
         switch (cacheHealth) {
            case FAILED:
               cacheInfo.status = ComponentStatus.FAILED.toString();
               break;
            case INITIALIZING:
               cacheInfo.status = ComponentStatus.INITIALIZING.toString();
               break;
            default:
               Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
               cacheInfo.status = cache.getStatus().toString();
               break;
         }
      }

      // if any of those are null, don't keep trying to retrieve the rebalancing status
      if (finalLocalTopologyManager != null && finalClusterRebalancingEnabled != null) {
         Boolean perCacheRebalancing = null;
         if (finalClusterRebalancingEnabled) {
            // if the global rebalancing is enabled, retrieve each cache status
            try {
               perCacheRebalancing = finalLocalTopologyManager.isCacheRebalancingEnabled(cacheName);
               if (perCacheRebalancing) {
                  StateTransferManager stateTransferManager = SecurityActions.getCacheComponent(cacheManager, cacheName, StateTransferManager.class);
                  cacheInfo.rebalancingRequested = stateTransferManager.getInflightSegmentTransferCount();
                  cacheInfo.rebalancingInflight = stateTransferManager.getInflightTransactionalSegmentCount();
               }
            } catch (Exception ex) {
               // There was an error retrieving this value. Just ignore
            }
         } else {
            // set all to false. global disabled rebalancing disables all caches rebalancing
            perCacheRebalancing = Boolean.FALSE;
         }

         cacheInfo.rebalancingEnabled = perCacheRebalancing;
      }

      return cacheInfo;
   }

   static class CacheInfo implements JsonSerialization {
      String status;
      String name;
      String type;
      boolean simpleCache;
      boolean transactional;
      boolean persistent;
      boolean bounded;
      boolean indexed;
      boolean secured;
      boolean hasRemoteBackup;
      boolean tracing;
      HealthStatus health;
      Boolean rebalancingEnabled;
      long rebalancingRequested;
      long rebalancingInflight;
      Collection<String> aliases;
      Stats stats;
      Integer size;
      String configuration;
      Boolean rehashInProgress;
      Boolean indexingInProgress;
      boolean statistics;
      Boolean queryable;
      MediaType keyStorage;
      MediaType valueStorage;
      String mode;
      StorageType storageType;
      String maxSize;
      long maxSizeBytes;

      @Override
      public Json toJson() {
         Json json = Json.object()
               .set("status", status)
               .set("name", name)
               .set("type", type)
               .set("simple_cache", simpleCache)
               .set("transactional", transactional)
               .set("persistent", persistent)
               .set("bounded", bounded)
               .set("secured", secured)
               .set("indexed", indexed)
               .set("has_remote_backup", hasRemoteBackup)
               .set("tracing", tracing)
               .set("health", health)
               .set("aliases", aliases)
               .set("statistics", statistics)
               .set("key_storage", keyStorage)
               .set("value_storage", valueStorage)
               .set("aliases", aliases)
               .set("mode", mode);

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
            json.set("rebalancing_requested", rebalancingRequested);
            json.set("rebalancing_inflight", rebalancingInflight);
         }
         if (configuration != null) {
            json.set("configuration", Json.factory().raw(configuration));
         }
         if (storageType != null) {
            json.set("storage_type", storageType)
                  .set("max_size", maxSize == null ? "" : maxSize)
                  .set("max_size_bytes", maxSizeBytes);
         }
         return json;
      }
   }

   @SuppressWarnings("rawtypes")
   protected CompletionStage<RestResponse> getSourceConnection(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      PersistenceManager persistenceManager =
            SecurityActions.getCacheComponent(invocationHelper.getRestCacheManager().getInstance(), cache.getName(), PersistenceManager.class);

      List<RemoteStore> remoteStores = new ArrayList<>(persistenceManager.getStores(RemoteStore.class));

      if (remoteStores.isEmpty()) {
         builder.status(NOT_FOUND);
         return completedFuture(builder.build());
      }

      if (remoteStores.size() != 1) {
         throw Log.REST.multipleRemoteStores();
      }

      RemoteStoreConfiguration storeConfiguration = remoteStores.get(0).getConfiguration();

      builder.entity(SerializationUtils.toJson(storeConfiguration));
      return completedFuture(builder.build());
   }

   protected CompletionStage<RestResponse> hasSourceConnections(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = ComponentRegistry.componentOf(cache, RollingUpgradeManager.class);

      return CompletableFuture.supplyAsync(() -> {
         if (!upgradeManager.isConnected(MIGRATOR_NAME)) {
            builder.status(NOT_FOUND);
         }
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> deleteSourceConnection(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      builder.status(NO_CONTENT);

      String cacheName = request.variables().get("cacheName");

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = ComponentRegistry.componentOf(cache, RollingUpgradeManager.class);
      return CompletableFuture.supplyAsync(() -> {
         if (upgradeManager.isConnected(MIGRATOR_NAME)) {
            upgradeManager.disconnectSource(MIGRATOR_NAME);
         } else {
            builder.status(HttpResponseStatus.NOT_MODIFIED);
         }

         return builder.build();
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> addSourceConnection(RestRequest request) {
      final NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      builder.status(NO_CONTENT);

      String cacheName = request.variables().get("cacheName");
      ContentSource contents = request.contents();
      byte[] config = contents.rawContent();

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);

      if (cache == null) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }

      if (config == null || config.length == 0) {
         return invocationHelper.newResponse(request, BAD_REQUEST, "A remote-store config must be provided").toFuture();
      }

      String storeConfig = new String(config, UTF_8);
      Json read = Json.read(storeConfig);

      if (!read.isObject() || read.at("remote-store") == null || read.asMap().size() != 1) {
         return invocationHelper.newResponse(request, BAD_REQUEST, "Invalid remote-store JSON description: a single remote-store element must be provided").toFuture();
      }

      return CompletableFuture.supplyAsync(() -> {
         RollingUpgradeManager upgradeManager = ComponentRegistry.componentOf(cache, RollingUpgradeManager.class);
         try {
            RemoteStoreConfiguration storeConfiguration = SerializationUtils.fromJson(read.toString());
            if (!upgradeManager.isConnected(MIGRATOR_NAME)) {
               upgradeManager.connectSource(MIGRATOR_NAME, storeConfiguration);
            } else {
               builder.status(HttpResponseStatus.NOT_MODIFIED);
            }
            return builder.build();
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> syncData(RestRequest request) {
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");
      String readBatchReq = request.getParameter("read-batch");
      String threadsReq = request.getParameter("threads");

      int readBatch = readBatchReq == null ? 10_000 : Integer.parseInt(readBatchReq);
      if (readBatch < 1) {
         throw Log.REST.illegalArgument("read-batch", readBatch);
      }
      int threads = request.getParameter("threads") == null ? ProcessorInfo.availableProcessors() : Integer.parseInt(threadsReq);
      if (threads < 1) {
         throw Log.REST.illegalArgument("threads", threads);
      }

      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      RollingUpgradeManager upgradeManager = ComponentRegistry.componentOf(cache, RollingUpgradeManager.class);

      return CompletableFuture.supplyAsync(() -> {
         long hotrod = upgradeManager.synchronizeData(MIGRATOR_NAME, readBatch, threads);
         builder.entity(Log.REST.synchronizedEntries(hotrod));
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> convert(RestRequest request, MediaType toType) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));
      String contents = request.contents().asString();

      if (contents == null || contents.isEmpty()) {
         throw Log.REST.missingContent();
      }
      return CompletableFuture.supplyAsync(() -> {
         ParserRegistry parserRegistry = invocationHelper.getParserRegistry();
         Properties properties = new Properties();
         ConfigurationReader reader = ConfigurationReader.from(contents)
               .withResolver(ConfigurationResourceResolvers.DEFAULT)
               .withType(request.contentType())
               .withProperties(properties)
               .withNamingStrategy(NamingStrategy.KEBAB_CASE).build();
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder();
         parserRegistry.parse(reader, holder);
         Map.Entry<String, ConfigurationBuilder> entry = holder.getNamedConfigurationBuilders().entrySet().iterator().next();
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

   protected CompletionStage<RestResponse> convert(RestRequest request) {
      return convert(request, negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML));
   }

   protected CompletionStage<RestResponse> compare(RestRequest request) {
      boolean ignoreMutable = Boolean.parseBoolean(request.getParameter("ignoreMutable"));
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      MediaType contentType = request.contentType();
      if (!contentType.match(MediaType.MULTIPART_FORM_DATA)) {
         throw Log.REST.wrongMediaType(MediaType.MULTIPART_FORM_DATA_TYPE, contentType.toString());
      }
      FullHttpRequest nettyRequest = ((NettyRestRequest) request).getFullHttpRequest();
      DefaultHttpDataFactory factory = new DefaultHttpDataFactory(false);
      HttpPostMultipartRequestDecoder decoder = new HttpPostMultipartRequestDecoder(factory, nettyRequest);
      try {
         List<InterfaceHttpData> datas = decoder.getBodyHttpDatas();
         if (datas.size() != 2) {
            throw Log.REST.cacheCompareWrongContent();
         }
         MemoryAttribute one = (MemoryAttribute) datas.get(0);
         MemoryAttribute two = (MemoryAttribute) datas.get(1);
         String s1 = one.content().toString(UTF_8);
         String s2 = two.content().toString(UTF_8);
         ParserRegistry parserRegistry = invocationHelper.getParserRegistry();
         Map<String, ConfigurationBuilder> b1 = parserRegistry.parse(s1, null).getNamedConfigurationBuilders();
         Map<String, ConfigurationBuilder> b2 = parserRegistry.parse(s2, null).getNamedConfigurationBuilders();
         if (b1.size() != 1 || b2.size() != 1) {
            throw Log.REST.cacheCompareWrongContent();
         }
         Configuration c1 = b1.values().iterator().next().build();
         Configuration c2 = b2.values().iterator().next().build();
         boolean result;
         if (ignoreMutable) {
            try {
               c1.validateUpdate(null, c2);
               result = true;
            } catch (Throwable t) {
               result = false;
               responseBuilder.entity(unwrapExceptionMessage(filterCause(t)));
            }
         } else {
            result = c1.equals(c2);
         }
         return CompletableFuture.completedFuture(responseBuilder.status(result ? NO_CONTENT : CONFLICT).build());
      } finally {
         decoder.destroy();
      }
   }

   protected CompletionStage<RestResponse> streamKeys(RestRequest request) {
      String cacheName = request.variables().get("cacheName");

      String batchParam = request.getParameter("batch");
      String limitParam = request.getParameter("limit");
      int batch = batchParam == null || batchParam.isEmpty() ? STREAM_BATCH_SIZE : Integer.parseInt(batchParam);
      int limit = limitParam == null || limitParam.isEmpty() ? -1 : Integer.parseInt(limitParam);

      AdvancedCache<Object, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, TEXT_PLAIN, MATCH_ALL, request);
      if (cache == null)
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }

      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);

      ComponentRegistry registry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      ClusterPublisherManager<Object, ?> cpm = registry.getClusterPublisherManager().wired();
      EncoderKeyMapper<Object> mapper = new EncoderKeyMapper<>(cache.getKeyDataConversion());
      mapper.injectDependencies(registry);
      SegmentPublisherSupplier<Object> sps = cpm.keyPublisher(null, null, null, EMPTY_BIT_SET, DeliveryGuarantee.EXACTLY_ONCE,
            batch, PublisherTransformers.identity());
      Flowable<byte[]> flowable = Flowable.fromPublisher(sps.publisherWithoutSegments())
            .map(e -> CacheChunkedStream.readContentAsBytes(mapper.apply(e)));
      if (limit > -1) {
         flowable = flowable.take(limit);
      }
      responseBuilder.entity(new CacheKeyStreamProcessor(flowable));

      responseBuilder.contentType(APPLICATION_JSON_TYPE);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   protected CompletionStage<RestResponse> streamEntries(RestRequest request) {
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
      if (cache == null) return invocationHelper.newResponse(request, NOT_FOUND).toFuture();

      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }

      final MediaType keyMediaType = getMediaType(negotiate, cache, true);
      final MediaType valueMediaType = getMediaType(negotiate, cache, false);

      AdvancedCache<?, ?> typedCache = invocationHelper.getRestCacheManager().getCache(cacheName, keyMediaType, valueMediaType, request);
      ComponentRegistry registry = SecurityActions.getCacheComponentRegistry(typedCache);
      ClusterPublisherManager<Object, Object> cpm = registry.getClusterPublisherManager().wired();
      InternalEntryFactory ief = registry.getInternalEntryFactory().running();
      EncoderEntryMapper<Object, Object, CacheEntry<Object, Object>> mapper = EncoderEntryMapper.newCacheEntryMapper(typedCache.getKeyDataConversion(), typedCache.getValueDataConversion(), ief);
      mapper.injectDependencies(registry);
      SegmentPublisherSupplier<CacheEntry<Object, Object>> sps = cpm.entryPublisher(null, null, null, EMPTY_BIT_SET, DeliveryGuarantee.EXACTLY_ONCE,
            batch, PublisherTransformers.identity());
      Flowable<CacheEntry<?, ?>> flowable = Flowable.fromPublisher(sps.publisherWithoutSegments())
            .map(mapper::apply);
      if (limit > -1) {
         flowable = flowable.take(limit);
      }
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      responseBuilder.entity(new CacheEntryStreamProcessor(flowable, keyMediaType.match(APPLICATION_JSON),
            valueMediaType.match(APPLICATION_JSON), metadata));

      responseBuilder.contentType(APPLICATION_JSON_TYPE);
      responseBuilder.header(ResponseHeader.KEY_CONTENT_TYPE_HEADER.getValue(), keyMediaType.toString());
      responseBuilder.header(ResponseHeader.VALUE_CONTENT_TYPE_HEADER.getValue(), valueMediaType.toString());

      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   CompletionStage<RestResponse> cacheListen(RestRequest request) {
      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, TEXT_PLAIN);
      String cacheName = request.variables().get("cacheName");
      boolean includeCurrentState = Boolean.parseBoolean(request.getParameter("includeCurrentState"));
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      Cache<?, ?> cache = restCacheManager.getCache(cacheName, accept, accept, request);
      BaseCacheListener listener = includeCurrentState ? new StatefulCacheListener(cache) : new StatelessCacheListener(cache);
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
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

   protected CompletionStage<RestResponse> removeCache(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();

      return CompletableFuture.supplyAsync(() -> {
         restCacheManager.getCacheManagerAdmin(request).removeCache(cacheName);
         return invocationHelper.newResponse(request)
               .status(OK)
               .build();
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> cacheExists(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");

      if (!invocationHelper.getRestCacheManager().getInstance().cacheConfigurationExists(cacheName)) {
         responseBuilder.status(NOT_FOUND);
      } else {
         responseBuilder.status(NO_CONTENT);
      }
      return CompletableFuture.completedFuture(responseBuilder.build());
   }

   CompletableFuture<RestResponse> createOrUpdate(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
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
            throw Log.REST.wrongMethod(request.method().toString());
         }
         String templateName = template.iterator().next();
         return CompletableFuture.supplyAsync(() -> {
            var config = SERVER_TEMPLATES.get(templateName);
            if (config != null) {
               administration.createCache(cacheName, config);
            } else {
               administration.createCache(cacheName, templateName);
            }
            responseBuilder.status(OK);
            return responseBuilder.build();
         }, invocationHelper.getExecutor());
      }

      ContentSource contents = request.contents();
      byte[] bytes = contents.rawContent();
      if (bytes == null || bytes.length == 0) {
         if (request.method() == PUT) {
            throw Log.REST.wrongMethod(request.method().toString());
         }
         return CompletableFuture.supplyAsync(() -> {
            administration.createCache(cacheName, (String) null);
            responseBuilder.status(OK);
            return responseBuilder.build();
         }, invocationHelper.getExecutor());
      }
      MediaType sourceType = request.contentType() == null ? APPLICATION_JSON : request.contentType();
      if (!sourceType.match(APPLICATION_JSON) && !sourceType.match(APPLICATION_XML) && !sourceType.match(APPLICATION_YAML) && !sourceType.match(TEXT_PLAIN)) {
         responseBuilder.status(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(invocationHelper.getRestCacheManager().getInstance());
      return CompletableFuture.supplyAsync(() -> {
         ConfigurationBuilderHolder holder = invocationHelper.getParserRegistry().parse(new String(bytes, UTF_8), sourceType);
         ConfigurationBuilder cfgBuilder = holder.getCurrentConfigurationBuilder() != null ? holder.getCurrentConfigurationBuilder() : new ConfigurationBuilder();
         if (request.method() == PUT) {
            administration.getOrCreateCache(cacheName, cfgBuilder.build(globalConfiguration));
         } else {
            administration.createCache(cacheName, cfgBuilder.build(globalConfiguration));
         }
         responseBuilder.status(OK);
         return responseBuilder.build();
      }, invocationHelper.getExecutor());
   }


   protected CompletionStage<RestResponse> getCacheStats(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      return CompletableFuture.supplyAsync(() ->
            asJsonResponse(invocationHelper.newResponse(request), cache.getAdvancedCache().getStats().toJson(), isPretty(request)), invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> resetCacheStats(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      return CompletableFuture.supplyAsync(() -> {
         cache.getAdvancedCache().getStats().reset();
         return invocationHelper.newResponse(request, NO_CONTENT);
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> getCacheDistribution(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<?> cache = invocationHelper.getRestCacheManager();
      boolean pretty = isPretty(request);
      return CompletableFuture.supplyAsync(() -> cache.cacheDistribution(cacheName, request), invocationHelper.getExecutor())
            .thenCompose(Function.identity())
            .thenApply(distributions -> asJsonResponse(invocationHelper.newResponse(request), Json.array(distributions.stream().map(CacheDistributionInfo::toJson).toArray()), pretty));
   }

   protected CompletionStage<RestResponse> getKeyDistribution(RestRequest request) {
      boolean pretty = isPretty(request);
      return keyDistribution(request)
            .thenApply(distribution -> asJsonResponse(invocationHelper.newResponse(request), distribution.toJson(), pretty));
   }

   protected CompletionStage<RestResponse> getAllDetails(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      boolean pretty = isPretty(request);
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();

      return CompletableFuture.supplyAsync(() -> getDetailResponse(request, cache, pretty), invocationHelper.getExecutor());
   }

   protected RestResponse getDetailResponse(RestRequest request, Cache<?, ?> cache, boolean pretty) {
      // We escalate privileges to obtain various items of the configuration, but we need to take care about
      // the details included in the response
      Configuration configuration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      GlobalConfiguration globalConfiguration = SecurityActions.getCacheManagerConfiguration(cacheManager);
      PersistenceManager persistenceManager = SecurityActions.getCacheComponent(cacheManager, cache.getName(), PersistenceManager.class);
      Stats stats = null;
      Boolean rehashInProgress = null;
      Boolean indexingInProgress = null;
      Boolean queryable = null;

      ComponentRegistry ccr = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      try {
         stats = ccr.getComponent(ClusterCacheStats.class);
         DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
         rehashInProgress = distributionManager != null && distributionManager.isRehashInProgress();
      } catch (SecurityException ex) {
         // Admin is needed
      }

      Boolean rebalancingEnabled = null;
      long rebalancingRequested = 0;
      long rebalancingInflight = 0;
      try {
         LocalTopologyManager localTopologyManager = ccr.getComponent(LocalTopologyManager.class);
         if (localTopologyManager != null) {
            rebalancingEnabled = localTopologyManager.isCacheRebalancingEnabled(cache.getName());
            StateTransferManager stateTransferManager = ccr.getComponent(StateTransferManager.class);
            rebalancingRequested = stateTransferManager.getInflightSegmentTransferCount();
            rebalancingInflight = stateTransferManager.getInflightTransactionalSegmentCount();
         }
      } catch (Exception ex) {
         // Getting rebalancing status might raise an exception
      }

      boolean globalTracingEnabled = globalConfiguration.tracing().enabled();

      Integer size = null;
      if (globalConfiguration.metrics().accurateSize()) {
         try {
            size = cache.size();
         } catch (SecurityException ex) {
            // Bulk Read is needed
         }
      }

      indexingInProgress = reindexingInProgress(cache);
      queryable = invocationHelper.getRestCacheManager().isCacheQueryable(cache);

      boolean statistics = configuration.statistics().enabled();
      boolean indexed = configuration.indexing().enabled();

      CacheInfo fullDetail = new CacheInfo();
      fullDetail.stats = stats;
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(pretty).build()) {
         invocationHelper.getParserRegistry().serialize(w, cache.getName(), configuration);
      }
      // Only include the full configuration if ADMIN
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache.getAdvancedCache());
      if (authorizationManager == null || authorizationManager.isPermissive()) {
         // Cache is not secured, use the global authz
         if (invocationHelper.getRestCacheManager().getAuthorizer().getPermissions(null, request.getSubject()).contains(AuthorizationPermission.ADMIN)) {
            fullDetail.configuration = sw.toString();
            fullDetail.storageType = configuration.memory().storage();
            fullDetail.maxSize = configuration.memory().maxSize();
            fullDetail.maxSizeBytes = configuration.memory().maxSizeBytes();
         }
      } else {
         authorizationManager.doIf(request.getSubject(), AuthorizationPermission.ADMIN, () -> {
            fullDetail.configuration = sw.toString();
            fullDetail.storageType = configuration.memory().storage();
            fullDetail.maxSize = configuration.memory().maxSize();
            fullDetail.maxSizeBytes = configuration.memory().maxSizeBytes();
         });
      }

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
      fullDetail.rebalancingRequested = rebalancingRequested;
      fullDetail.rebalancingInflight = rebalancingInflight;
      fullDetail.keyStorage = cache.getAdvancedCache().getKeyDataConversion().getStorageMediaType();
      fullDetail.valueStorage = cache.getAdvancedCache().getValueDataConversion().getStorageMediaType();
      fullDetail.mode = configuration.clustering().cacheModeString();
      fullDetail.tracing = globalTracingEnabled && configuration.tracing().enabled();
      fullDetail.aliases = configuration.aliases();
      fullDetail.name = cache.getName();
      fullDetail.status = cache.getStatus().name();
      fullDetail.type = configuration.clustering().cacheMode().toCacheType();
      return addEntityAsJson(fullDetail.toJson(), invocationHelper.newResponse(request), pretty).build();
   }

   private boolean reindexingInProgress(Cache<?,?> cache) {
      SearchStatsRetriever searchStatsRetriever = ComponentRegistryUtils.getSearchStatsRetriever(cache);
      if (searchStatsRetriever != null || !internalCacheRegistry.isInternalCache(cache.getName())) {
         return searchStatsRetriever.getSearchStatistics().getIndexStatistics().reindexing();
      }

      // safely returning false in case of internal cache
      return false;
   }

   protected CompletionStage<RestResponse> getCacheConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));

      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML);
      responseBuilder.contentType(accept);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());
      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(subjectCacheManager, cacheName);
      ByteArrayOutputStream entity = new ByteArrayOutputStream();
      try (ConfigurationWriter writer = ConfigurationWriter.to(entity).withType(accept).prettyPrint(pretty).build()) {
         parserRegistry.serialize(writer, cacheName, cacheConfiguration);
      } catch (Exception e) {
         throw Util.unchecked(e);
      }
      responseBuilder.entity(entity);
      return CompletableFuture.completedFuture(responseBuilder.status(OK).build());
   }

   protected CompletionStage<RestResponse> getCacheAvailability(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      // Use EmbeddedCacheManager directly to allow internal caches to be updated
      if (!invocationHelper.getRestCacheManager().getInstance().isRunning(cacheName))
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getInstance().getCache(cacheName).getAdvancedCache();
      if (cache == null) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }
      AvailabilityMode availability = cache.getAvailability();
      return CompletableFuture.completedFuture(
            invocationHelper.newResponse(request)
                  .entity(availability)
                  .contentType(TEXT_PLAIN)
                  .status(OK)
                  .build()
      );
   }

   protected CompletionStage<RestResponse> setCacheAvailability(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      String availability = request.getParameter("availability");
      // Use EmbeddedCacheManager directly to allow internal caches to be updated
      if (!invocationHelper.getRestCacheManager().getInstance().isRunning(cacheName))
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getInstance().getCache(cacheName).getAdvancedCache();
      if (cache == null) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }
      try {
         AvailabilityMode availabilityMode = AvailabilityMode.valueOf(availability.toUpperCase());
         cache.setAvailability(availabilityMode);
         return CompletableFuture.completedFuture(invocationHelper.newResponse(request).status(NO_CONTENT).build());
      } catch (IllegalArgumentException e) {
         return invocationHelper.newResponse(request, BAD_REQUEST, String.format("Unknown AvailabilityMode '%s'", availability)).toFuture();
      }
   }

   protected CompletionStage<RestResponse> getCacheConfigMutableAttributes(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");
      boolean full = Boolean.parseBoolean(request.getParameter("full"));

      responseBuilder.contentType(APPLICATION_JSON);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
      }
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();

      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      Map<String, Attribute<?>> attributes = new LinkedHashMap<>();
      mutableAttributes(cacheConfiguration, attributes, null);
      if (full) {
         Json all = Json.object();
         for (Map.Entry<String, Attribute<?>> entry : attributes.entrySet()) {
            Attribute<?> attribute = entry.getValue();
            Class<?> type = attribute.getAttributeDefinition().getType();
            Json object = Json.object("value", attribute.get(), "type", type.getSimpleName().toLowerCase());
            if (type.isEnum()) {
               object.set("universe", Arrays.stream(type.getEnumConstants()).map(Object::toString).collect(Collectors.toList()));
            }
            all.set(entry.getKey(), object);
         }
         return asJsonResponseFuture(invocationHelper.newResponse(request), all, isPretty(request));
      } else {
         return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(attributes.keySet()), isPretty(request));
      }
   }

   private static void mutableAttributes(ConfigurationElement<?> element, Map<String, Attribute<?>> attributes, String prefix) {
      if (prefix == null) {
         prefix = "";
      } else if (prefix.isEmpty()) {
         prefix = element.elementName() + '.';
      } else {
         prefix = prefix + element.elementName() + '.';
      }
      for (Attribute<?> attribute : element.attributes().attributes()) {
         if (!attribute.isImmutable()) {
            AttributeDefinition<?> definition = attribute.getAttributeDefinition();
            // even if mutable, we don't want expose this attribute to the user
            if (AbstractTypedPropertiesConfiguration.PROPERTIES.equals(definition)) {
               continue;
            }
            attributes.put(prefix + definition.name(), attribute);
         }
      }
      for (ConfigurationElement<?> child : element.children()) {
         mutableAttributes(child, attributes, prefix);
      }
   }

   protected CompletionStage<RestResponse> getCacheConfigMutableAttribute(RestRequest request) {
      String attributeName = request.getParameter("attribute-name");
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }
      Configuration cacheConfiguration = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      Attribute<?> attribute = cacheConfiguration.findAttribute(attributeName);
      if (attribute.isImmutable()) {
         throw Log.REST.immutableAttribute(attributeName);
      } else {
         return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(String.valueOf(attribute.get())), isPretty(request));
      }
   }

   protected CompletionStage<RestResponse> setCacheConfigMutableAttribute(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String attributeName = request.getParameter("attribute-name");
      String attributeValue = String.join(" ", request.parameters().get("attribute-value"));
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }
      EmbeddedCacheManagerAdmin administration = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      return CompletableFuture.supplyAsync(() -> {
         administration.updateConfigurationAttribute(cacheName, attributeName, attributeValue);
         return responseBuilder.status(OK).build();
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> assignAlias(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String alias = request.getParameter("alias");
      String cacheName = request.variables().get("cacheName");
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }
      invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      EmbeddedCacheManagerAdmin administration = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      return CompletableFuture.supplyAsync(() -> {
         administration.assignAlias(alias, cacheName);
         return responseBuilder.status(OK).build();
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> getSize(RestRequest request) {
      String cacheName = request.variables().get("cacheName");

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      boolean pretty = isPretty(request);
      return cache.sizeAsync().thenApply(size -> asJsonResponse(invocationHelper.newResponse(request), Json.make(size), pretty));
   }

   protected CompletionStage<RestResponse> getCacheNames(RestRequest request) throws RestResponseException {
      Collection<String> cacheNames = invocationHelper.getRestCacheManager().getAccessibleCacheNames();
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(cacheNames), isPretty(request));
   }

   protected CompletionStage<RestResponse> getCacheHealth(RestRequest request) throws RestResponseException {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<?> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      EmbeddedCacheManager subjectCacheManager = cacheManager.withSubject(request.getSubject());
      List<CacheHealth> cachesHealth = SecurityActions.getHealth(subjectCacheManager).getCacheHealth(Set.of(cacheName));
      if (cachesHealth.isEmpty()) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }

      return completedFuture(
            invocationHelper.newResponse(request)
                  .contentType(TEXT_PLAIN)
                  .entity(cachesHealth.get(0).getStatus().toString())
                  .status(OK)
                  .build()
      );
   }

   protected CompletionStage<RestResponse> setRebalancing(boolean enable, RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      if (!restCacheManager.cacheExists(cacheName))
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();

      return CompletableFuture.supplyAsync(() -> {
         NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
         LocalTopologyManager ltm = SecurityActions.getGlobalComponentRegistry(restCacheManager.getInstance()).getLocalTopologyManager();
         try {
            ltm.setCacheRebalancingEnabled(cacheName, enable);
            builder.status(NO_CONTENT);
         } catch (Exception e) {
            throw Util.unchecked(e);
         }
         return builder.build();
      }, invocationHelper.getExecutor());
   }

   protected CompletionStage<RestResponse> reinitializeCache(RestRequest request) {
      boolean force = Boolean.parseBoolean(request.getParameter("force"));
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();
      EmbeddedCacheManager ecm = invocationHelper.getProtocolServer().getCacheManager();
      if (!ecm.isCoordinator()) {
         builder.status(BAD_REQUEST);
         builder.entity(Json.make("Node not coordinator, request at " + ecm.getCoordinator()));
         return CompletableFuture.completedFuture(builder.build());
      }

      GlobalComponentRegistry gcr = SecurityActions.getGlobalComponentRegistry(ecm);
      ClusterTopologyManager ctm = gcr.getClusterTopologyManager();
      LocalTopologyManager ltm = gcr.getLocalTopologyManager();
      if (ctm == null || ltm == null) {
         builder.status(BAD_REQUEST);
         return CompletableFuture.completedFuture(builder.build());
      }

      builder.status(NO_CONTENT);
      String cache = request.variables().get("cacheName");
      InternalCacheRegistry internalRegistry = gcr.getComponent(InternalCacheRegistry.class);
      if (internalRegistry.isInternalCache(cache)) {
         return CompletableFuture.completedFuture(builder
               .status(BAD_REQUEST)
               .entity(Json.make(String.format("Cache '%s' is internal", cache)))
               .build());
      }
      if (ecm.isRunning(cache)) return CompletableFuture.completedFuture(builder.build());
      if (!ecm.cacheExists(cache)) {
         return CompletableFuture.completedFuture(builder
               .status(NOT_FOUND)
               .entity(Json.make(String.format("Cache '%s' does not exist", cache)))
               .build());
      }

      return CompletableFuture.supplyAsync(() -> {
         if (!ctm.useCurrentTopologyAsStable(cache, force))
            return CompletableFuture.completedFuture(builder.build());

         // We wait for the topology to be installed.
         return ltm.stableTopologyCompletion(cache)
             .thenApply(ignore -> builder.build());
      }, invocationHelper.getExecutor())
          .thenCompose(Function.identity())
          .thenApply(Function.identity());
   }

   public abstract static class BaseCacheListener {
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

   protected CompletionStage<RestResponse> getCacheNamesPerRole(RestRequest request) throws RestResponseException {
      String roleName = request.getParameter("role");
      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);

      if (roleName == null) {
         return completedFuture(builder.status(HttpResponseStatus.BAD_REQUEST).build());
      }

      GlobalAuthorizationConfiguration authorization = invocationHelper.getRestCacheManager()
            .getInstance().getCacheManagerConfiguration().security().authorization();
      Role role = authorization.getRole(roleName);

      if (role == null) {
         return completedFuture(builder.status(NOT_FOUND).build());
      }

      // secured caches
      Set<String> cachesNames = new HashSet<>(invocationHelper.getRestCacheManager().getAccessibleCacheNames());
      cachesNames.removeAll(internalCacheRegistry.getInternalCacheNames());

      Set<String> securedCaches = new HashSet<>();
      Set<String> nonSecuredCaches = new HashSet<>();

      cachesNames.stream().forEach(cacheName -> {
         try {
            AdvancedCache cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
            SecurityConfiguration config = cache.getCacheConfiguration().security();
            if (config.authorization().enabled() && config.authorization().roles().contains(roleName)) {
               securedCaches.add(cacheName);
            } else if (!config.authorization().enabled()){
               nonSecuredCaches.add(cacheName);
            }
         } catch (Exception ex) {
            // the cache might be invalid
         }
      });
      Json json = Json.object();
      json.set("secured", Json.make(securedCaches));
      json.set("non-secured", Json.make(nonSecuredCaches));
      return asJsonResponseFuture(builder, Json.make(json), isPretty(request));
   }
}
