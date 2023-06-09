package org.infinispan.server.functional.hotrod;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_OFF_HEAP;
import static org.infinispan.server.functional.XSiteIT.MAX_COUNT_KEYS;
import static org.infinispan.server.functional.XSiteIT.NR_KEYS;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 **/
public class XSiteHotRodCacheOperations {

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = XSiteIT.SERVERS;

   @Test
   public void testHotRodOperations() {
      String lonXML = String.format(XSiteIT.LON_CACHE_XML_CONFIG, SERVERS.getMethodName());
      RemoteCache<String, String> lonCache = SERVERS.hotrod(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).create();
      RemoteCache<String, String> nycCache = SERVERS.hotrod(NYC).create(); //nyc cache don't backup to lon

      insertAndVerifyEntries(lonCache, nycCache, false);
   }

   @Test
   public void testHotRodOperationsWithDifferentCacheName() {
      RemoteCache<String, String> lonCache = SERVERS.hotrod(LON)
            .createRemoteCacheManager()
            .administration()
            .createCache("lon-cache", new StringConfiguration(LON_CACHE_CUSTOM_NAME_XML_CONFIG));

      RemoteCache<String, String> nycCache = SERVERS.hotrod(NYC)
            .createRemoteCacheManager()
            .administration()
            .createCache("nyc-cache", new StringConfiguration(NYC_CACHE_CUSTOM_NAME_XML_CONFIG));

      insertAndVerifyEntries(lonCache, nycCache, true);
   }

   @Test
   public void testHotRodOperationsWithOffHeapFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, SERVERS.getMethodName());
      RemoteCache<Integer, Integer> lonCache = SERVERS.hotrod(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).create();
      RemoteCache<Integer, Integer> nycCache = SERVERS.hotrod(NYC).create(); //nyc cache don't backup to lon

      //Just to make sure that the file store is empty
      assertEquals(0, getTotalMemoryEntries(lonXML));

      IntStream.range(0, NR_KEYS).forEach(i -> lonCache.put(i, i));

      eventuallyEquals(NR_KEYS, nycCache::size);
      assertEquals(MAX_COUNT_KEYS, getTotalMemoryEntries(lonXML));
   }

   @Test
   public void testMultimap() {
      String multimapCacheName = "multimap";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.sites().addBackup()
            .site(NYC).strategy(BackupConfiguration.BackupStrategy.SYNC).backupFailurePolicy(BackupFailurePolicy.WARN);
      builder.sites().addBackup()
            .site(LON).strategy(BackupConfiguration.BackupStrategy.SYNC).backupFailurePolicy(BackupFailurePolicy.WARN);
      SERVERS.hotrod(LON).createRemoteCacheManager().administration().getOrCreateCache(multimapCacheName, builder.build());
      SERVERS.hotrod(NYC).createRemoteCacheManager().administration().getOrCreateCache(multimapCacheName, builder.build());


      RemoteMultimapCache<String, String> lonCache = multimapCache(LON, multimapCacheName);
      RemoteMultimapCache<String, String> nycCache = multimapCache(NYC, multimapCacheName);

      String key = Util.threadLocalRandomUUID().toString();
      Collection<String> values = createValues(4);
      storeMultimapValues(lonCache, key, values);
      assertMultimapData(lonCache, key, values);
      assertMultimapData(nycCache, key, values);

      key = Util.threadLocalRandomUUID().toString();
      values = createValues(5);
      storeMultimapValues(nycCache, key, values);
      assertMultimapData(lonCache, key, values);
      assertMultimapData(nycCache, key, values);
   }

   private void assertMultimapData(RemoteMultimapCache<String, String> cache, String key, Collection<String> values) {
      Collection<String> data = cache.get(key).join();
      assertEquals(values.size(), data.size());
      for (String v : values) {
         assertTrue(data.contains(v));
      }
   }

   private RemoteMultimapCache<String, String> multimapCache(String site, String cacheName) {
      MultimapCacheManager<String, String> multimapCacheManager = SERVERS.getMultimapCacheManager(site);
      return multimapCacheManager.get(cacheName);
   }

   private static void storeMultimapValues(RemoteMultimapCache<String, String> rmc, String key, Collection<String> values) {
      for (String v : values) {
         rmc.put(key, v).join();
      }
   }

   private static List<String> createValues(int size) {
      List<String> values = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         values.add(Util.threadLocalRandomUUID().toString());
      }
      return values;
   }

   private int getTotalMemoryEntries(String lonXML) {
      RestClient restClient = SERVERS.rest(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).get();

      RestCacheClient client = restClient.cache(SERVERS.getMethodName());
      Json json = Json.read(assertStatus(OK, client.stats()));
      return json.asJsonMap().get("current_number_of_entries_in_memory").asInteger();
   }

   private void insertAndVerifyEntries(RemoteCache<String, String> lonCache, RemoteCache<String, String> nycCache, boolean allSitesBackup) {
      lonCache.put("k1", "v1");
      nycCache.put("k2", "v2");

      assertEquals("v1", lonCache.get("k1"));
      eventuallyEquals("v1", () -> nycCache.get("k1"));
      if(allSitesBackup) {
         eventuallyEquals("v2", () -> lonCache.get("k2"));
      } else {
         assertNull(lonCache.get("k2"));
      }
      assertEquals ("v2", nycCache.get("k2"));
   }
}
