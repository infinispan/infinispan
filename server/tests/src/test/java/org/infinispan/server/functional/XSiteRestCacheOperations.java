package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_OFF_HEAP;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.NUM_SERVERS;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_XML_CONFIG;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
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

      assertStatus(NOT_FOUND, nycCache.xsiteBackups());
      assertResponse(OK, lonCache.backupStatus(NYC), r -> assertEquals(NUM_SERVERS, Json.read(r.getBody()).asMap().size()));
      assertStatus(NOT_FOUND, nycCache.backupStatus(LON));

      assertResponse(OK, lonCache.xsiteBackups(), r -> {
         Json lonXsiteBackups = Json.read(r.getBody());
         assertEquals("online", lonXsiteBackups.asJsonMap().get(NYC).asJsonMap().get("status").asString());

      });
      assertStatus(OK, lonCache.takeSiteOffline(NYC));
      assertResponse(OK, lonCache.xsiteBackups(), r -> {
         Json lonXsiteBackups = Json.read(r.getBody());
         assertTrue(lonXsiteBackups.asJsonMap().get(NYC).asJsonMap().get("status").asString().contains("offline"));
      });
      assertStatus(OK, lonCache.bringSiteOnline(NYC));
      assertResponse(OK, lonCache.xsiteBackups(), r -> {
         Json lonXsiteBackups = Json.read(r.getBody());
         assertTrue(lonXsiteBackups.asJsonMap().get(NYC).asJsonMap().get("status").asString().contains("online"));
      });
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
               assertStatus(NO_CONTENT, lonCache.put(s, s));
            });
      eventuallyEquals("300", () -> assertStatus(OK, nycCache.size()));
      assertEquals(100, getTotalMemoryEntries(lonCache));
   }

   private int getTotalMemoryEntries(RestCacheClient restCache) {
      Json json = Json.read(assertStatus(OK, restCache.stats()));
      return json.asJsonMap().get("current_number_of_entries_in_memory").asInteger();
   }

   private void insertAndVerifyEntries(boolean allSitesBackup) {
      assertStatus(NO_CONTENT, lonCache.put("k1", "v1"));
      assertStatus(NO_CONTENT, nycCache.put("k2", "v2"));
      assertEquals("v1", assertStatus(OK, lonCache.get("k1")));
      eventuallyEquals("v1", () -> assertStatus(OK, nycCache.get("k1")));
      assertEquals("v2", assertStatus(OK, nycCache.get("k2")));
      if (allSitesBackup) {
         eventuallyEquals("v2", () -> assertStatus(OK, lonCache.get("k2")));
      } else {
         assertStatus(NOT_FOUND, lonCache.get("k2"));
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
