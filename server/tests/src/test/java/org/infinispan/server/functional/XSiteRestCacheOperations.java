package org.infinispan.server.functional;

import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.server.functional.XSiteIT.LON;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_OFF_HEAP;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.NUM_SERVERS;
import static org.infinispan.server.functional.XSiteIT.NYC;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_XML_CONFIG;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
import org.infinispan.util.concurrent.CompletionStages;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 **/
public class XSiteRestCacheOperations {

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = XSiteIT.SERVERS;

   @Rule
   public InfinispanXSiteServerTestMethodRule SERVER_TEST = new InfinispanXSiteServerTestMethodRule(SERVERS);

   private static void assertStatus(int status, CompletionStage<RestResponse> stage) {
      assertEquals(status, CompletionStages.join(stage).getStatus());
   }

   private static String bodyOf(CompletionStage<RestResponse> stage) {
      RestResponse rsp = CompletionStages.join(stage);
      return rsp.getStatus() == 200 ? rsp.getBody() : null;
   }

   String cacheName;
   private RestCacheClient lonCache;
   private RestCacheClient nycCache;

   @Before
   public void setup() {
      cacheName = SERVER_TEST.getMethodName();
   }

   @Test
   public void testRestOperationsLonToNycBackup() {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, cacheName);
      lonCache = createRestCacheClient(LON, lonXML);
      nycCache = createRestCacheClient(NYC);

      //nyc doesn't backup to lon
      insertAndVerifyEntries(false);
   }

   @Test
   public void testRestOperationsAllSitesBackup() {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, cacheName);
      String nycXML = String.format(NYC_CACHE_XML_CONFIG, cacheName);
      lonCache = createRestCacheClient(LON, lonXML);
      nycCache = createRestCacheClient(NYC, nycXML);

      insertAndVerifyEntries(true);
   }

   @Test
   public void testBackupStatus() {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, cacheName);
      lonCache = createRestCacheClient(LON, lonXML);
      nycCache = createRestCacheClient(NYC);

      assertNull(bodyOf(nycCache.xsiteBackups()));
      assertEquals(NUM_SERVERS, Json.read(bodyOf(lonCache.backupStatus(NYC))).asMap().size());
      assertNull(bodyOf(nycCache.backupStatus(LON)));

      Json lonXsiteBackups = Json.read(bodyOf(lonCache.xsiteBackups()));
      assertEquals("online", lonXsiteBackups.asJsonMap().get(NYC).asJsonMap().get("status").asString());

      CompletionStages.join(lonCache.takeSiteOffline(NYC));
      lonXsiteBackups = Json.read(bodyOf(lonCache.xsiteBackups()));
      assertTrue(lonXsiteBackups.asJsonMap().get(NYC).asJsonMap().get("status").asString().contains("offline"));
      CompletionStages.join(lonCache.bringSiteOnline(NYC));
      lonXsiteBackups = Json.read(bodyOf(lonCache.xsiteBackups()));
      assertTrue(lonXsiteBackups.asJsonMap().get(NYC).asJsonMap().get("status").asString().contains("online"));
   }

   @Test
   public void testWithDifferentCacheNames() {
      lonCache = createRestCacheClient(LON, LON_CACHE_CUSTOM_NAME_XML_CONFIG);
      nycCache = createRestCacheClient(NYC, NYC_CACHE_CUSTOM_NAME_XML_CONFIG);

      insertAndVerifyEntries(true);
   }

   @Test
   public void testHotRodOperationsWithOffHeapSingleFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, cacheName);
      lonCache = createRestCacheClient(LON, lonXML);
      nycCache = createRestCacheClient(NYC);

      //Just to make sure that the file store is empty
      assertEquals(0, getTotalMemoryEntries(lonCache));

      IntStream.range(0, 300)
               .forEach(i -> {
                  String s = Integer.toString(i);
                  bodyOf(lonCache.put(s, s));
      });

      eventuallyEquals("300", () -> bodyOf(nycCache.size()));
      assertEquals(100, getTotalMemoryEntries(lonCache));
   }

   private int getTotalMemoryEntries(RestCacheClient restCache) {
      Json json = Json.read(sync(restCache.stats()).getBody());
      return json.asJsonMap().get("current_number_of_entries_in_memory").asInteger();
   }

   private void insertAndVerifyEntries(boolean allSitesBackup) {
      assertStatus(204, lonCache.put("k1", "v1"));
      assertStatus(204, nycCache.put("k2", "v2"));
      assertEquals("v1", bodyOf(lonCache.get("k1")));
      eventuallyEquals("v1", ()-> bodyOf(nycCache.get("k1")));
      assertEquals("v2", bodyOf(nycCache.get("k2")));
      if (allSitesBackup) {
         eventuallyEquals("v2", ()-> bodyOf(lonCache.get("k2")));
      } else {
         assertEquals(null, bodyOf(lonCache.get("k2")));
      }
   }

   private RestCacheClient createRestCacheClient(String siteName, String xml) {
      RestCacheClient cache = SERVER_TEST.rest(siteName).get().cache(cacheName);
      assertStatus(200, cache.createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, xml)));
      return cache;
   }

   private RestCacheClient createRestCacheClient(String siteName) {

      RestCacheClient cache = SERVER_TEST.rest(siteName).get().cache(cacheName);
      assertStatus(200, cache.createWithTemplate(DefaultTemplate.DIST_SYNC.getTemplateName()));
      return cache;
   }

}
