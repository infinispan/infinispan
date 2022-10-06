package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_OFF_HEAP;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 **/
public class XSiteHotRodCacheOperations {

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = XSiteIT.SERVERS;

   @Rule
   public InfinispanXSiteServerTestMethodRule SERVER_TEST = new InfinispanXSiteServerTestMethodRule(SERVERS);

   @Test
   public void testHotRodOperations() {
      String lonXML = String.format(XSiteIT.LON_CACHE_XML_CONFIG, SERVER_TEST.getMethodName());
      RemoteCache<String, String> lonCache = SERVER_TEST.hotrod(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).create();
      RemoteCache<String, String> nycCache = SERVER_TEST.hotrod(NYC).create(); //nyc cache don't backup to lon

      insertAndVerifyEntries(lonCache, nycCache, false);
   }

   @Test
   public void testHotRodOperationsWithDifferentCacheName() {
      RemoteCache<String, String> lonCache = SERVER_TEST.hotrod(LON)
            .createRemoteCacheManager()
            .administration()
            .createCache("lon-cache", new StringConfiguration(LON_CACHE_CUSTOM_NAME_XML_CONFIG));

      RemoteCache<String, String> nycCache = SERVER_TEST.hotrod(NYC)
            .createRemoteCacheManager()
            .administration()
            .createCache("nyc-cache", new StringConfiguration(NYC_CACHE_CUSTOM_NAME_XML_CONFIG));

      insertAndVerifyEntries(lonCache, nycCache, true);
   }

   @Test
   public void testHotRodOperationsWithOffHeapFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, SERVER_TEST.getMethodName());
      RemoteCache<Integer, Integer> lonCache = SERVER_TEST.hotrod(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).create();
      RemoteCache<Integer, Integer> nycCache = SERVER_TEST.hotrod(NYC).create(); //nyc cache don't backup to lon

      //Just to make sure that the file store is empty
      assertEquals(0, getTotalMemoryEntries(lonXML));

      IntStream.range(0, 300).forEach(i -> lonCache.put(i, i));

      eventuallyEquals(300, nycCache::size);
      assertEquals(100, getTotalMemoryEntries(lonXML));
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
      SERVER_TEST.hotrod(LON).createRemoteCacheManager().administration().getOrCreateCache(multimapCacheName, builder.build());
      SERVER_TEST.hotrod(NYC).createRemoteCacheManager().administration().getOrCreateCache(multimapCacheName, builder.build());


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
      Assert.assertEquals(values.size(), data.size());
      for (String v : values) {
         Assert.assertTrue(data.contains(v));
      }
   }

   private RemoteMultimapCache<String, String> multimapCache(String site, String cacheName) {
      MultimapCacheManager<String, String> multimapCacheManager = SERVER_TEST.getMultimapCacheManager(site);
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
      RestClient restClient = SERVER_TEST.rest(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).get();

      RestCacheClient client = restClient.cache(SERVER_TEST.getMethodName());
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
