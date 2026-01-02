package org.infinispan.rest.resources;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Tests for REST v3 Cache API endpoints.
 * This class extends CacheResourceV2Test and overrides the endpoint map to use V3 paths.
 *
 * @since 16.0
 */
@Test(groups = "functional", testName = "rest.CacheResourceV3Test")
public class CacheResourceV3Test extends CacheResourceV2Test {

   // Use a different persistent location to avoid conflicts with V2 tests
   {
      PERSISTENT_LOCATION = tmpDirectory(CacheResourceV3Test.class.getName());
   }

   @Override
   protected String parameters() {
      return "[security=" + security + ", protocol=" + protocol.toString() + ", ssl=" + ssl + ", cacheMode=" + cacheMode + ", browser=" + browser + "]";
   }

   @Override
   protected CacheResourceV2Test withCacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   @Override
   protected CacheResourceV2Test createTestInstance() {
      return new CacheResourceV3Test();
   }

   @Override
   public Object[] factory() {
      // Reuse parent's factory implementation
      return super.factory();
   }

   @Override
   protected String getIndexedCacheStoreName() {
      return "storeV3";
   }

   // Override the endpoint map with V3 paths
   {
      endpoints = Map.ofEntries(
            // Cache collection operations
            Map.entry("cacheList", "/v3/caches"),
            Map.entry("detailedCacheList", "/v3/meta/caches/_detailed"),
            Map.entry("roleAccessibleCaches", "/v3/meta/caches/_role-accessible?role={role}"),

            // Cache lifecycle operations
            Map.entry("cacheCreate", "/v3/caches/{cacheName}"),
            Map.entry("cacheUpdate", "/v3/caches/{cacheName}"),
            Map.entry("cacheDelete", "/v3/caches/{cacheName}"),
            Map.entry("cacheExists", "/v3/caches/{cacheName}"),

            // Cache configuration operations
            Map.entry("cacheConfig", "/v3/caches/{cacheName}/config"),
            Map.entry("cacheConfigAttributes", "/v3/caches/{cacheName}/config/attributes?full=true"),
            Map.entry("cacheConfigAttribute", "/v3/caches/{cacheName}/config/attributes/{attribute}"),
            Map.entry("cacheConfigAttributeUpdate", "/v3/caches/{cacheName}/config/attributes/{attribute}"),

            // Cache entry operations
            Map.entry("cacheEntryGet", "/v3/caches/{cacheName}/entries/{key}"),
            Map.entry("cacheEntryPut", "/v3/caches/{cacheName}/entries/{key}"),
            Map.entry("cacheEntryPost", "/v3/caches/{cacheName}/entries/{key}"),
            Map.entry("cacheEntryHead", "/v3/caches/{cacheName}/entries/{key}"),
            Map.entry("cacheEntryDelete", "/v3/caches/{cacheName}/entries/{key}"),

            // Cache statistics operations
            Map.entry("cacheStats", "/v3/caches/{cacheName}/_stats"),
            Map.entry("cacheStatsReset", "/v3/caches/{cacheName}/_stats-reset"),
            Map.entry("cacheDistribution", "/v3/caches/{cacheName}/_distribution"),
            Map.entry("keyDistribution", "/v3/caches/{cacheName}/_distribution/{key}"),

            // Cache data operations
            Map.entry("cacheClear", "/v3/caches/{cacheName}/_clear"),
            Map.entry("cacheSize", "/v3/caches/{cacheName}/_size"),
            Map.entry("cacheKeys", "/v3/caches/{cacheName}/keys"),
            Map.entry("cacheEntries", "/v3/caches/{cacheName}/entries"),

            // Cache health and availability
            Map.entry("cacheHealth", "/v3/caches/{cacheName}/_health"),
            Map.entry("cacheAvailabilityGet", "/v3/caches/{cacheName}/_availability"),
            Map.entry("cacheAvailabilitySet", "/v3/caches/{cacheName}/_availability"),

            // Cache rebalancing operations
            Map.entry("rebalancingEnable", "/v3/caches/{cacheName}/_rebalancing-enable"),
            Map.entry("rebalancingDisable", "/v3/caches/{cacheName}/_rebalancing-disable"),

            // Cache details and metadata
            Map.entry("cacheDetails", "/v3/caches/{cacheName}/details"),
            Map.entry("cacheAssignAlias", "/v3/caches/{cacheName}/_assign-alias?alias={alias}"),

            // Search operations
            Map.entry("cacheQuery", "/v3/caches/{cacheName}/_search"),
            Map.entry("cacheDeleteByQuery", "/v3/caches/{cacheName}/_delete-by-query"),
            Map.entry("cacheIndexMetamodel", "/v3/caches/{cacheName}/_index-metamodel"),
            Map.entry("cacheSearchStats", "/v3/caches/{cacheName}/_search-stats"),
            Map.entry("cacheSearchStatsClear", "/v3/caches/{cacheName}/_search-stats-clear"),

            // Rolling upgrade operations
            Map.entry("rollingUpgradeSourceConnect", "/v3/caches/{cacheName}/rolling-upgrade/source-connection"),
            Map.entry("rollingUpgradeSourceGet", "/v3/caches/{cacheName}/rolling-upgrade/source-connection"),
            Map.entry("rollingUpgradeSourceDelete", "/v3/caches/{cacheName}/rolling-upgrade/source-connection"),
            Map.entry("rollingUpgradeSourceExists", "/v3/caches/{cacheName}/rolling-upgrade/source-connection"),
            Map.entry("rollingUpgradeSyncData", "/v3/caches/{cacheName}/_sync-data"),

            // Cache listeners
            Map.entry("cacheListen", "/v3/caches/{cacheName}/_listen"),

            // Cache reinitialize
            Map.entry("cacheReinitialize", "/v3/caches/{cacheName}/_initialize"),

            // Configuration conversion and comparison
            Map.entry("configConvert", "/v3/_cache-config-convert"),
            Map.entry("configCompare", "/v3/_cache-config-compare"),

            // Schema operations (schemas are not versioned, they remain the same)
            Map.entry("schemaPut", "/v2/schemas/{schemaName}"),
            Map.entry("schemaList", "/v2/schemas"),

            // Container operations (container is not versioned, it remains the same)
            Map.entry("containerHealth", "/v2/container/health")
      );
   }
}
