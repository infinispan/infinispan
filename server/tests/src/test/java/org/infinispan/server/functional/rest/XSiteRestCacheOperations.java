package org.infinispan.server.functional.rest;

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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 **/
public class XSiteRestCacheOperations {

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = XSiteIT.SERVERS;

   @Test
   public void testRestOperationsLonToNycBackup() {
      String lonXML = String.format(LON_CACHE_CONFIG, SERVERS.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVERS.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVERS.getMethodName());

      //nyc doesn't backup to lon
      insertAndVerifyEntries(false, lonCache, nycCache);
   }

   @Test
   public void testRestOperationsAllSitesBackup() {
      String lonXML = String.format(LON_CACHE_CONFIG, SERVERS.getMethodName());
      String nycXML = String.format(NYC_CACHE_CONFIG, SERVERS.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVERS.getMethodName(), lonXML);
      RestCacheClient nycCache = createRestCacheClient(NYC, SERVERS.getMethodName(), nycXML);

      insertAndVerifyEntries(true, lonCache, nycCache);
   }

   @Test
   public void testBackupStatus() {
      String lonXML = String.format(LON_CACHE_CONFIG, SERVERS.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVERS.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVERS.getMethodName());

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
   public void testHotRodOperationsWithOffHeapSingleFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, SERVERS.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVERS.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVERS.getMethodName());

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
      RestCacheClient cache = SERVERS.rest(siteName).get().cache(cacheName);
      assertStatus(200, cache.createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, xml)));
      return cache;
   }

   private RestCacheClient createDefaultRestCacheClient(String siteName, String cacheName) {
      RestCacheClient cache = SERVERS.rest(siteName).get().cache(cacheName);
      assertStatus(200, cache.createWithTemplate(DefaultTemplate.DIST_SYNC.getTemplateName()));
      return cache;
   }

}
