package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_YAML;
import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.HEAD;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.RequestHeader;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.openapi.ParameterIn;
import org.infinispan.rest.framework.openapi.Schema;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.core.query.json.JsonQueryRequest;
import org.infinispan.server.core.query.json.JsonQueryResponse;
import org.infinispan.telemetry.InfinispanTelemetry;

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

   public static final String CACHE_NOT_FOUND_RESPONSE = "Cache not found";
   public static final String ILLEGAL_ARGUMENTS_RESPONSE = "Illegal argument values";

   public CacheResourceV3(InvocationHelper invocationHelper, InfinispanTelemetry telemetryService) {
      super(invocationHelper, telemetryService);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("cache", "Cache API")
            // Collection-level operations
            .invocation().methods(GET).path("/v3/caches").name("List available caches")
               .operationId("CacheList")
               .response(OK, "Cache list as a JSON array", APPLICATION_JSON, new Schema(String[].class))
               .response(OK, "Cache list as HTML", TEXT_HTML)
               .handleWith(this::getCacheNames)

            // Meta operations on caches (using /v3/meta/caches namespace to avoid conflicts with cache names)
            .invocation().methods(GET).path("/v3/meta/caches/_detailed").name("List available caches with details")
               .operationId("DetailedCacheList")
               .response(OK, "Detailed cache list with metadata", APPLICATION_JSON)
               .handleWith(this::getCaches)
            .invocation().methods(GET).path("/v3/meta/caches/_role-accessible")
               .operationId("RoleAccessibleCaches")
               .name("List caches accessible by a specific role")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
               .parameter("role", ParameterIn.QUERY, true, Schema.STRING, "The role name")
               .response(OK, "Caches accessible by the role, separated into secured and non-secured", APPLICATION_JSON)
               .response(BAD_REQUEST, "Role parameter is missing", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, "Role not found", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheNamesPerRole)

            // Utility operations (not cache-specific)
            .invocation().methods(POST).path("/v3/_cache-config-convert")
               .operationId("convertCacheConfig")
               .name("Convert cache configurations between formats")
               .request("Cache configuration", true, Map.of(
                     APPLICATION_XML, Schema.STRING,
                     APPLICATION_JSON, Schema.STRING,
                     APPLICATION_YAML, Schema.STRING
               ))
               .handleWith(this::convert)
            .invocation().methods(POST).path("/v3/_cache-config-compare")
               .operationId("compareCacheConfig")
               .name("Compare cache configurations")
               .handleWith(this::compare)

            // Key related operations
            .invocation().methods(PUT, POST).path("/v3/caches/{cacheName}/entries/{cacheKey}")
               .name("Put/update an entry in a cache")
               .operationId("CacheEntry")
               .parameter(RequestHeader.KEY_CONTENT_TYPE_HEADER, ParameterIn.HEADER, false, Schema.STRING, "The content type for the key")
               .parameter(RequestHeader.TTL_SECONDS_HEADER, ParameterIn.HEADER, false, Schema.INTEGER, "The time-to-live (TTL) of the entry in seconds")
               .parameter(RequestHeader.MAX_TIME_IDLE_HEADER, ParameterIn.HEADER, false, Schema.INTEGER, "The maximum idle time in seconds")
               .request("Entry value", true, Map.of(MATCH_ALL, Schema.NONE))
               .response(NO_CONTENT, "Entry was stored")
               .response(CONFLICT, "ETag conflict", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::putValueToCache)
            .invocation().methods(GET, HEAD).path("/v3/caches/{cacheName}/entries/{cacheKey}")
               .operationId("CacheEntry")
               .name("Retrieve an entry from a cache")
               .parameter(RequestHeader.EXTENDED_HEADER, ParameterIn.HEADER, false, Schema.BOOLEAN, "Whether to return additional information about the entry in the response headers")
               .response(OK, "Entry value", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheValue)
            .invocation().method(GET).path("/v3/caches/{cacheName}/_distribution/{cacheKey}")
               .operationId("KeyDistribution")
               .name("Retrieve key distribution")
               .response(OK, "The key distribution", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getKeyDistribution)
            .invocation().method(DELETE).path("/v3/caches/{cacheName}/entries/{cacheKey}")
               .operationId("CacheEntry")
               .name("Delete an entry from a cache")
               .response(NO_CONTENT, "Entry was deleted")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::deleteCacheValue)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/keys")
               .operationId("AllCacheKeys")
               .name("Retrieve all keys from a cache")
               .response(OK, "All cache keys", APPLICATION_JSON, Schema.STRING_ARRAY)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::streamKeys)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/entries")
               .operationId("AllCacheEntries")
               .name("Retrieve all entries from a cache")
               .response(OK, "All cache entries", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::streamEntries)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_listen")
               .operationId("CacheEvents")
               .name("Receive events from a cache")
               .response(OK, "Cache events", TEXT_EVENT_STREAM)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::cacheListen)
            // Config
            .invocation().methods(GET, HEAD).path("/v3/caches/{cacheName}/config")
               .operationId("CacheConfig")
               .name("Retrieve cache configuration")
               .permission(AuthorizationPermission.ADMIN)
               .response(OK, "Cache configuration as XML", APPLICATION_XML)
               .response(OK, "Cache configuration as JSON", APPLICATION_JSON)
               .response(OK, "Cache configuration as YAML", APPLICATION_YAML)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheConfig)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/config/attributes")
               .operationId("AllCacheAttributes")
               .name("Retrieve all mutable configuration attributes for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .parameter("full", ParameterIn.QUERY, false, Schema.BOOLEAN, "Retrieve all mutable attributes and their values")
               .response(OK, "Mutable cache attributes", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheConfigMutableAttributes)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/config/attributes/{attribute}")
               .operationId("CacheAttribute")
               .name("Retrieve the value of a single mutable configuration attribute for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .response(OK, "The value of the requested cache configuration attribute", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/config/attributes/{attribute}")
               .operationId("CacheAttribute")
               .name("Sets the value of a single mutable configuration attribute for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .response(NO_CONTENT, "The cache configuration attribute was successfully changed")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::setCacheConfigMutableAttribute)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_assign-alias")
               .operationId("assignCacheAlias")
               .permission(AuthorizationPermission.ADMIN)
               .parameter("alias", ParameterIn.QUERY, true, Schema.STRING, "Assign an alias to a cache")
               .response(OK, "Alias assigned", TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, ILLEGAL_ARGUMENTS_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::assignAlias)

            // Stats
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_stats")
               .operationId("CacheStats")
               .name("Retrieve cache statistics")
               .response(OK, "The cache statistics", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheStats)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_stats-reset")
               .operationId("resetCacheStats")
               .name("Reset cache statistics")
               .permission(AuthorizationPermission.ADMIN)
               .response(NO_CONTENT, "The cache statistics were reset")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::resetCacheStats)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_distribution")
               .operationId("CacheDistribution")
               .response(OK, "The cache distribution", APPLICATION_JSON)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheDistribution)

            // Health
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_health")
               .operationId("CacheHealth")
               .name("Retrieve the cache health")
               .response(OK, "Cache health status", TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheHealth)

            // Cache lifecycle
            .invocation().methods(POST, PUT).path("/v3/caches/{cacheName}")
               .operationId("Cache")
               .name("Creates a cache or updates its configuration")
               .request("The cache configuration", true, sequencedMap(Map.entry(APPLICATION_JSON, Schema.NONE), Map.entry(APPLICATION_XML, Schema.NONE), Map.entry(APPLICATION_YAML, Schema.NONE)))
               .response(OK, "The cache was created or its configuration updated", TEXT_PLAIN)
               .response(BAD_REQUEST, ILLEGAL_ARGUMENTS_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::createOrUpdate)
            .invocation().method(DELETE).path("/v3/caches/{cacheName}")
               .operationId("Cache")
               .name("Deletes a cache")
               .response(OK, "The cache was removed", TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::removeCache)
            .invocation().method(HEAD).path("/v3/caches/{cacheName}")
               .operationId("cacheExists")
               .name("Determines if a cache exists")
               .response(NO_CONTENT, "The cache exists")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::cacheExists)

            .invocation().method(GET).path("/v3/caches/{cacheName}/_availability")
               .operationId("CacheAvailability")
               .name("Retrieves the cache availability")
               .permission(AuthorizationPermission.ADMIN)
               .response(OK, "The cache availability status", TEXT_PLAIN)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getCacheAvailability)
            .invocation().method(POST).path("/v3/caches/{cacheName}/_availability")
               .operationId("CacheAvailability")
               .name("Sets the cache availability")
               .permission(AuthorizationPermission.ADMIN)
               .response(NO_CONTENT, "The cache availability status was set")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::setCacheAvailability)

            // Operations
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_clear")
               .operationId("clearCache")
               .name("Clears a cache")
               .response(NO_CONTENT, "The cache was cleared")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::clearEntireCache)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_size")
               .operationId("CacheSize")
               .name("Retrieves the number of entries in the cache")
               .response(OK, "The number of entries in the cace", APPLICATION_JSON, Schema.LONG)
               .handleWith(this::getSize)

            // Rolling Upgrade methods
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_sync-data")
               .operationId("syncData")
               .name("Synchronizes data between the source and target clusters")
               .parameter("read-batch", ParameterIn.QUERY, false, Schema.INTEGER, "Read batch size. Defaults to 10000.")
               .parameter("threads", ParameterIn.QUERY, false, Schema.INTEGER, "Number of threads to use for synchronization. Defaults to the number of available cores.")
               .response(OK, "Data was synchronized successfully", TEXT_PLAIN)
               .response(BAD_REQUEST, ILLEGAL_ARGUMENTS_RESPONSE, TEXT_PLAIN)
               .handleWith(this::syncData)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .operationId("addSourceConnection")
               .name("Adds a source cluster connection for cache data migration")
               .request("The remote store configuration", true, Map.of(APPLICATION_JSON, Schema.NONE))
               .response(NO_CONTENT, "The source connection was added")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .response(BAD_REQUEST, ILLEGAL_ARGUMENTS_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::addSourceConnection)
            .invocation().methods(DELETE).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .operationId("removeSourceConnection")
               .name("Deletes a source cluster connection for cache data migration")
               .response(NO_CONTENT, "The source cluster was disconnected")
               .response(NOT_MODIFIED, "No change was made", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::deleteSourceConnection)
            .invocation().methods(HEAD).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .operationId("sourceConnectionExists")
               .name("Checks whether a source cluster connection exists for a cache")
               .response(OK, "The cache is connected to a source cluster", TEXT_PLAIN)
               .response(NOT_FOUND, "The cache does not exists or is not connected to a source cluster", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::hasSourceConnections)
            .invocation().methods(GET).path("/v3/caches/{cacheName}/rolling-upgrade/source-connection")
               .operationId("SourceConnection")
               .name("Retrieves a source cluster connection for a cache")
               .response(OK, "The remote store configuration for the source cluster", APPLICATION_JSON, Schema.NONE)
               .response(NOT_FOUND, "The cache does not exist or is not connected to a source cluster", TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getSourceConnection)

            // Search
            .invocation().methods(GET).path("/v3/caches/{cacheName}/_search")
               .operationId("queryCache")
               .name("Performs an Ickle query on a cache")
               .permission(AuthorizationPermission.BULK_READ)
               .parameter("query", ParameterIn.QUERY, true, Schema.STRING, "The Ickle query")
               .parameter("offset", ParameterIn.QUERY, false, Schema.INTEGER, "The offset")
               .parameter("max_results", ParameterIn.QUERY, false, Schema.INTEGER, "The maximum number of results")
               .parameter("hit_count_accuracy", ParameterIn.QUERY, false, Schema.INTEGER, "The hit count accuracy")
               .response(OK, "The results of the query", APPLICATION_JSON, new Schema(JsonQueryResponse.class))
               .handleWith(queryAction::search)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_search")
               .operationId("QueryCache")
               .name("Performs an Ickle query on a cache")
               .permission(AuthorizationPermission.BULK_READ)
               .request("The query request object", true, Map.of(APPLICATION_JSON, new Schema(JsonQueryRequest.class)))
               .response(OK, "The results of the query", APPLICATION_JSON, new Schema(JsonQueryResponse.class))
               .handleWith(queryAction::search)
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_delete-by-query")
               .operationId("deleteByQuery")
               .name("Delete entries from a cache matching an Ickle query")
               .permission(AuthorizationPermission.BULK_WRITE)
               .request("The query request object", true, Map.of(APPLICATION_JSON, new Schema(JsonQueryRequest.class)))
               .response(OK, "The number of deleted entries", APPLICATION_JSON, Schema.LONG)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(queryAction::deleteByQuery)
            .invocation().methods(DELETE).path("/v3/caches/{cacheName}/_delete-by-query")
               .operationId("deleteByQueryDelete")
               .name("Delete entries from a cache matching an Ickle query (DELETE)")
               .permission(AuthorizationPermission.BULK_WRITE)
               .request("The query request object", true, Map.of(APPLICATION_JSON, new Schema(JsonQueryRequest.class)))
               .response(OK, "The number of deleted entries", APPLICATION_JSON, Schema.LONG)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(queryAction::deleteByQuery)

            // All details
            .invocation().methods(GET).path("/v3/caches/{cacheName}/details")
               .operationId("CacheDetails")
               .name("Retrieves details about a cache")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::getAllDetails)

            // Enable Rebalance
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_rebalancing-enable")
               .operationId("enableCacheRebalancing")
               .name("Enable rebalancing for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
               .response(NO_CONTENT, "Rebalancing was enabled")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(r -> setRebalancing(true, r))

            // Disable Rebalance
            .invocation().methods(POST).path("/v3/caches/{cacheName}/_rebalancing-disable")
               .operationId("disableCacheRebalancing")
               .name("Disable rebalancing for a cache")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
               .response(NO_CONTENT, "Rebalancing was disabled")
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(r -> setRebalancing(false, r))

            // Restore after a shutdown
            .invocation().method(POST).path("/v3/caches/{cacheName}/_initialize")
               .operationId("initializeCache")
               .name("Initialize cache")
               .permission(AuthorizationPermission.ADMIN)
               .auditContext(AuditContext.CACHE)
               .parameter("force", ParameterIn.QUERY, false, Schema.BOOLEAN, "Force use of the current topology")
               .response(NO_CONTENT, "Cache was initialized")
               .response(BAD_REQUEST, "Illegal state", TEXT_PLAIN, Schema.STRING)
               .response(NOT_FOUND, CACHE_NOT_FOUND_RESPONSE, TEXT_PLAIN, Schema.STRING)
               .handleWith(this::reinitializeCache)

            .create();
   }

   @SafeVarargs
   static <K, V> Map<K, V> sequencedMap(Map.Entry<? extends K, ? extends V>... entries) {
      Map<K, V> map = new LinkedHashMap<>(entries.length);
      for(Map.Entry<? extends K, ? extends V> entry : entries) {
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }
}
