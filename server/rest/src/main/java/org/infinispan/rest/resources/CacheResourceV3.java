package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
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
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SecurityConfiguration;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.ServerSentEvent;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.distribution.CacheDistributionInfo;
import org.infinispan.rest.distribution.CompleteKeyDistribution;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.rest.logging.Log;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Role;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.topology.LocalTopologyManager;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Flowable;

/**
 * REST resource to manage the caches.
 * Rules for OpenAPI v3 compliance:
 * <ul>
 *    <li>Resources, such as cache entries, should have unique paths</li>
 *    <li>Actions should be prefixed by _</li>
 * </ul>
 *
 * @since 16.0
 */
public class CacheResourceV3 extends CacheResourceV2 implements ResourceHandler {

   public static final String CACHE_NOT_FOUND_MESSAGE = "Cache not found";

   public CacheResourceV3(InvocationHelper invocationHelper, InfinispanTelemetry telemetryService) {
      super(invocationHelper, telemetryService);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("cache", "REST resource to manage caches.")
            // Key related operations
            .invocation().methods(PUT, POST).path("/v3/caches/{cacheName}/entries/{cacheKey}")
               .name("Put/update an entry in a cache")
               .parameter(RequestHeader.KEY_CONTENT_TYPE_HEADER, ParameterIn.HEADER, Schema.STRING, "The content type for the key")
               .parameter(RequestHeader.TTL_SECONDS_HEADER, ParameterIn.HEADER, Schema.INTEGER, "The time-to-live (TTL) of the entry in seconds")
               .parameter(RequestHeader.MAX_TIME_IDLE_HEADER, ParameterIn.HEADER, Schema.INTEGER, "The maximum idle time in seconds")
               .response(NO_CONTENT, "Entry was stored", TEXT_PLAIN)
               .response(CONFLICT, "ETag conflict", TEXT_PLAIN)
               .handleWith(this::putValueToCache)
            .invocation().methods(GET, HEAD).path("/v3/caches/{cacheName}/entries/{cacheKey}")
               .name("Retrieve an entry from a cache")
               .parameter(RequestHeader.EXTENDED_HEADER, ParameterIn.HEADER, Schema.BOOLEAN, "Whether to return additional information about the entry in the response headers")
               .handleWith(this::getCacheValue)
            .invocation().method(GET).path("/v3/caches/{cacheName}/_distribution/{cacheKey}")
               .name("Retrieve key distribution")
               .response(OK, "The key distribution", APPLICATION_JSON, new Schema(CompleteKeyDistribution.class))
               .handleWith(this::getKeyDistribution)
            .invocation().method(DELETE).path("/v3/caches/{cacheName}/entries/{cacheKey}")
               .name("Delete an entry from a cache")
               .handleWith(this::deleteCacheValue)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/keys")
               .name("Retrieve all keys from a cache")
               .response(OK, "All cache keys", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::streamKeys)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/entries")
               .name("Retrieve all entries from a cache")
               .response(OK, "All cache keys", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::streamEntries)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_listen")
               .name("Receive events from a cache")
               .response(OK, "Cache events", TEXT_EVENT_STREAM)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::cacheListen)
            // Config
            .invocation().methods(GET, HEAD).path("/v3/caches/{cacheName}/config").name("Retrieve cache configuration")
               .permission(AuthorizationPermission.ADMIN)
               .response(OK, "Cache configuration as XML", APPLICATION_XML)
               .response(OK, "Cache configuration as JSON", APPLICATION_JSON)
               .response(OK, "Cache configuration as YAML", APPLICATION_YAML)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/config/attributes")
               .name("Retrieve all mutable configuration attributes for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .parameter("full", ParameterIn.QUERY, Schema.BOOLEAN, "Retrieve all mutable attributes and their values")
               .response(OK, "Mutable cache attributes", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::getCacheConfigMutableAttributes)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/config/attributes/{attribute}")
               .name("Retrieve the value of a single mutable configuration attribute for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .response(OK, "The value of the requested cache configuration attribute", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::getCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/config/attributes/{attribute}")
               .name("Sets the value of a single mutable configuration attribute for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .handleWith(this::setCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_assign-alias")
               .permission(AuthorizationPermission.ADMIN)
               .handleWith(this::assignAlias)

            // Stats
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_stats")
               .name("Retrieve cache statistics")
               .response(OK, "The cache statistics", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::getCacheStats)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_stats-reset")
               .name("Reset cache statistics")
               .permission(AuthorizationPermission.ADMIN)
               .response(NO_CONTENT, "The cache statistics were reset", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::resetCacheStats)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_distribution")
               .response(OK, "The cache distribution", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN, new Schema(CacheDistributionInfo[].class))
               .handleWith(this::getCacheDistribution)

            // List
            .invocation().methods(GET).path("/v3/caches/").name("List available caches")
               .response(OK, "Cache list as a JSON array", APPLICATION_JSON, new Schema(String[].class))
               .response(OK, "Cache list as HTML", MediaType.TEXT_HTML)
               .handleWith(this::getCacheNames)

            // List of caches for role
            /*.invocation().methods(GET).path("/v3/caches").withAction("role-accessible")
            .permission(AuthorizationPermission.ADMIN).name("CACHES PER ROLE LIST").auditContext(AuditContext.CACHE)
            .handleWith(this::getCacheNamesPerRole)*/

            // Health
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_health")
               .name("Retrieve the cache health")
               .response(OK, "Cache health status", TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::getCacheHealth)

            // Cache lifecycle
            .invocation().methods(POST, PUT).path("/v3/caches/{cacheName}")
               .name("Creates or updates a cache configuration")
               .handleWith(this::createOrUpdate)
            .invocation().method(DELETE).path("/v3/caches/{cacheName}")
               .name("Deletes a cache")
               .response(OK, "Cache exists status", MediaType.TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::removeCache)
            .invocation().method(HEAD).path("/v3/caches/{cacheName}")
               .name("Determines if a cache exists")
               .response(OK, "Cache exists status", MediaType.TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_MESSAGE, MediaType.TEXT_PLAIN)
               .handleWith(this::cacheExists)

            .invocation().method(GET).path("/v3/caches/{cacheName}/_availability")
               .name("Retrieves the cache availability")
               .permission(AuthorizationPermission.ADMIN)
               .handleWith(this::getCacheAvailability)
            .invocation().method(POST).path("/v3/caches/{cacheName}/_availability")
               .name("Sets the cache availability")
               .permission(AuthorizationPermission.ADMIN)
               .handleWith(this::setCacheAvailability)

            // Operations
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_clear")
               .name("Clears a cache")
               .handleWith(this::clearEntireCache)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_size")
               .name("Retrieves the number of entries in the cache")
               .handleWith(this::getSize)

            // Rolling Upgrade methods
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_sync-data")
               .name("Synchronizes data between the source and target clusters")
               .handleWith(this::syncData)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .name("Adds a source cluster connection for cache data migration")
               .handleWith(this::addSourceConnection)
            .invocation().methods(DELETE).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .name("Deletes a source cluster connection for cache data migration")
               .handleWith(this::deleteSourceConnection)
            .invocation().methods(HEAD).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .name("Checks whether a source cluster connection exists for a cache")
               .handleWith(this::hasSourceConnections)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .name("Retrieves a source cluster connection for a cache")
               .handleWith(this::getSourceConnection)

            // Search
            .invocation().methods(GET, POST).path("/v3/caches/{cacheName}/_search")
               .name("Performs an Ickle query on a cache")
               .permission(AuthorizationPermission.BULK_READ)
               .handleWith(queryAction::search)

            // Misc
            .invocation().methods(POST).path("/v3/_convert-cache-config")
               .name("Convert cache configurations between formats")
               .handleWith(this::convert)
            .invocation().methods(POST).path("/v3/_compare-cache-config")
               .name("Compare cache configurations")
               .handleWith(this::compare)

            // All details
            .invocation().methods(GET).path("/v3/caches/{cacheName}/details")
               .name("Retrieves details about a cache")
               .handleWith(this::getAllDetails)

            // Enable Rebalance
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_enable-rebalancing")
               .name("Enable rebalancing for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
               .handleWith(r -> setRebalancing(true, r))

            // Disable Rebalance
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_disable-rebalancing")
               .name("Disable rebalancing for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
               .handleWith(r -> setRebalancing(false, r))

            // Restore after a shutdown
            .invocation().method(POST).path("/v3/caches/{cacheName}/_initialize")
               .name("Initialize cache")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
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

   private CompletionStage<RestResponse> cacheListen(RestRequest request) {
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

   private CompletableFuture<RestResponse> createOrUpdate(RestRequest request) {
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
            administration.createCache(cacheName, templateName);
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

   private CompletionStage<RestResponse> getAllDetails(RestRequest request) {
      String cacheName = request.variables().get("cacheName");
      boolean pretty = isPretty(request);
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null)
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();

      return CompletableFuture.supplyAsync(() -> getDetailResponse(request, cache, pretty), invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> getCacheConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String cacheName = request.variables().get("cacheName");
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));

      MediaType accept = negotiateMediaType(request, APPLICATION_JSON, APPLICATION_XML, APPLICATION_YAML);
      responseBuilder.contentType(accept);
      if (!invocationHelper.getRestCacheManager().getInstance().getCacheConfigurationNames().contains(cacheName)) {
         responseBuilder.status(NOT_FOUND).build();
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

   private CompletionStage<RestResponse> getCacheConfigMutableAttributes(RestRequest request) {
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

   private CompletionStage<RestResponse> getCacheConfigMutableAttribute(RestRequest request) {
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

   private CompletionStage<RestResponse> setCacheConfigMutableAttribute(RestRequest request) {
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

   private CompletionStage<RestResponse> getCacheHealth(RestRequest request) throws RestResponseException {
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

   private CompletionStage<RestResponse> getCacheNamesPerRole(RestRequest request) throws RestResponseException {
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

      cachesNames.forEach(cacheName -> {
         try {
            AdvancedCache cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
            SecurityConfiguration config = cache.getCacheConfiguration().security();
            if (config.authorization().enabled() && config.authorization().roles().contains(roleName)) {
               securedCaches.add(cacheName);
            } else if (!config.authorization().enabled()) {
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
