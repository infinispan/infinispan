package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CUSTOM_NAME_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_OFF_HEAP;
import static org.infinispan.server.functional.XSiteIT.MAX_COUNT_KEYS;
import static org.infinispan.server.functional.XSiteIT.NR_KEYS;
import static org.infinispan.server.functional.XSiteIT.NUM_SERVERS;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CONFIG;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CUSTOM_NAME_CONFIG;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
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

   String cacheName;
   private RestCacheClient lonCache;
   private RestCacheClient nycCache;

   @Before
   public void setup() {
      cacheName = SERVER_TEST.getMethodName();
   }

   @Test
   public void testRestOperationsLonToNycBackup() {
      String lonXML = String.format(LON_CACHE_CONFIG, SERVER_TEST.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVER_TEST.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVER_TEST.getMethodName());

      //nyc doesn't backup to lon
      insertAndVerifyEntries(false, lonCache, nycCache);
   }

   @Test
   public void testRestOperationsAllSitesBackup() {
      String lonXML = String.format(LON_CACHE_CONFIG, SERVER_TEST.getMethodName());
      String nycXML = String.format(NYC_CACHE_CONFIG, SERVER_TEST.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVER_TEST.getMethodName(), lonXML);
      RestCacheClient nycCache = createRestCacheClient(NYC, SERVER_TEST.getMethodName(), nycXML);

      insertAndVerifyEntries(true, lonCache, nycCache);
   }

   @Test
   public void testBackupStatus() {
      String lonXML = String.format(LON_CACHE_CONFIG, SERVER_TEST.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVER_TEST.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVER_TEST.getMethodName());

      assertStatus(NOT_FOUND, nycCache.xsiteBackups());
      assertResponse(OK, lonCache.backupStatus(NYC), r -> assertEquals(NUM_SERVERS, Json.read(r.getBody()).asMap().size()));
      assertStatus(NOT_FOUND, nycCache.backupStatus(LON));

      assertResponse(OK, lonCache.xsiteBackups(), r -> checkSiteStatus(r, NYC, "online"));
      assertStatus(OK, lonCache.takeSiteOffline(NYC));
      assertResponse(OK, lonCache.xsiteBackups(), r -> checkSiteStatus(r, NYC, "offline"));
      assertStatus(OK, lonCache.bringSiteOnline(NYC));
      assertResponse(OK, lonCache.xsiteBackups(), r -> checkSiteStatus(r, NYC, "online"));
   }

   @Test
   public void testWithDifferentCacheNames() {
      RestCacheClient lonCache = createRestCacheClient(LON, "lon-cache-rest", String.format(LON_CACHE_CUSTOM_NAME_CONFIG, "rest", "rest"));
      RestCacheClient nycCache = createRestCacheClient(NYC, "nyc-cache-rest", String.format(NYC_CACHE_CUSTOM_NAME_CONFIG, "rest", "rest"));
      assertResponse(OK, lonCache.xsiteBackups(), r -> checkSiteStatus(r, NYC, "online"));
      assertResponse(OK, nycCache.xsiteBackups(), r -> checkSiteStatus(r, LON, "online"));

      insertAndVerifyEntries(true, lonCache, nycCache);
   }

   private static void checkSiteStatus(RestResponse r, String site, String status) {
      Json backups = Json.read(r.getBody());
      assertEquals(status, backups.asJsonMap().get(site).asJsonMap().get("status").asString());
   }

   @Test
   public void testRestOperationsWithOffHeapSingleFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, SERVER_TEST.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVER_TEST.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVER_TEST.getMethodName());

      //Just to make sure that the file store is empty
      assertEquals(0, getTotalMemoryEntries(lonCache));

      IntStream.range(0, NR_KEYS)
            .mapToObj(Integer::toString)
            .forEach(s -> assertStatus(NO_CONTENT, lonCache.put(s, s)));
      eventuallyEquals(Integer.toString(NR_KEYS), () -> sync(nycCache.size()).getBody());
      assertEquals(MAX_COUNT_KEYS, getTotalMemoryEntries(lonCache));
   }

   private int getTotalMemoryEntries(RestCacheClient restCache) {
      Json json = Json.read(assertStatus(OK, restCache.stats()));
      return json.asJsonMap().get("current_number_of_entries_in_memory").asInteger();
   }

   private void insertAndVerifyEntries(boolean allSitesBackup, RestCacheClient lonCache, RestCacheClient nycCache) {
      assertStatus(NO_CONTENT, lonCache.put("k1", "v1"));
      assertStatus(NO_CONTENT, nycCache.put("k2", "v2"));
      assertEquals("v1", assertStatus(OK, lonCache.get("k1")));
      assertEquals("v2", assertStatus(OK, nycCache.get("k2")));
      eventuallyEquals("v1", () -> sync(nycCache.get("k1")).getBody());
      if (allSitesBackup) {
         eventuallyEquals("v2", () -> sync(lonCache.get("k2")).getBody());
      } else {
         assertStatus(NOT_FOUND, lonCache.get("k2"));
      }
   }

   private RestCacheClient createRestCacheClient(String siteName, String cacheName, String xml) {
      RestCacheClient cache = SERVER_TEST.rest(siteName).get().cache(cacheName);
      assertStatus(200, cache.createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, xml)));
      return cache;
   }

   private RestCacheClient createDefaultRestCacheClient(String siteName, String cacheName) {
      RestCacheClient cache = SERVER_TEST.rest(siteName).get().cache(cacheName);
      assertStatus(200, cache.createWithTemplate(DefaultTemplate.DIST_SYNC.getTemplateName()));
      return cache;
   }

}
