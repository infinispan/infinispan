package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.resources.SSEListener;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.logging.Log;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 **/
public class XSiteRestCacheOperations {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

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
      assertResponse(OK, lonCache.backupStatus(NYC), r -> assertEquals(NUM_SERVERS, Json.read(r.body()).asMap().size()));
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
      Json backups = Json.read(r.body());
      assertEquals(status, backups.asJsonMap().get(site).asJsonMap().get("status").asString());
   }

   @Test
   public void testWithOffHeapSingleFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, SERVERS.getMethodName());
      RestCacheClient lonCache = createRestCacheClient(LON, SERVERS.getMethodName(), lonXML);
      RestCacheClient nycCache = createDefaultRestCacheClient(NYC, SERVERS.getMethodName());

      //Just to make sure that the file store is empty
      assertEquals(0, getTotalMemoryEntries(lonCache));

      IntStream.range(0, NR_KEYS)
            .mapToObj(Integer::toString)
            .forEach(s -> assertStatus(NO_CONTENT, lonCache.put(s, s)));
      eventuallyEquals(Integer.toString(NR_KEYS), () -> sync(nycCache.size()).body());
      assertEquals(MAX_COUNT_KEYS, getTotalMemoryEntries(lonCache));
   }

   @ParameterizedTest(name = "{0}-{1}")
   @ArgumentsSource(RestOperations.ArgsProvider.class)
   public void testSSEvents(Protocol protocol, RestOperations.AcceptSerialization serialization) throws IOException, InterruptedException {
      var builder = new RestClientConfigurationBuilder().protocol(protocol);
      var client = SERVERS.rest(LON).withClientConfiguration(builder).create();

      // we are focusing in the cross-site events, so we can create the cache in advance.
      var cacheName = Util.threadLocalRandomUUID().toString();
      var cache = client.cache(cacheName);
      assertThat(cache.createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, String.format(LON_CACHE_CONFIG, cacheName)))).isOk();
      createDefaultRestCacheClient(NYC, cacheName);

      log.debug("Testing backwards compatible events");
      var url = "/rest/v2/container?action=listen";
      testCrossSiteEvents(url, client, cache, serialization, false, false);
      testCrossSiteEvents(url + "&pretty=true", client, cache, serialization, false, false);

      // all
      log.debug("Testing all events (no filter)");
      url = "/rest/v2/container?action=listen&category=all";
      testCrossSiteEvents(url, client, cache, serialization, true, true);
      testCrossSiteEvents(url + "&pretty=true", client, cache, serialization, true, true);

      for (var combination : Util.generatePowerSet(List.of("config", "lifecycle", "cluster", "security", "tasks", "cross-site"))) {
         // do not filter out empty
         log.debugf("Testing filter category=%s", String.join(",", combination));
         var cluster = combination.contains("cluster");
         var crossSite = combination.contains("cross-site");
         url = "/rest/v2/container?action=listen&category=%s".formatted(String.join(",", combination));
         testCrossSiteEvents(url, client, cache, serialization, cluster, crossSite);
         testCrossSiteEvents(url + "&pretty=true", client, cache, serialization, cluster, crossSite);
      }

      assertThat(cache.delete()).isOk();
   }

   @Test
   public void testJGroupsSSEvents() throws IOException, InterruptedException {
      // Restart NYC server to check if the JGroups events are properly processed, and the event log is triggered.
      var client = SERVERS.rest(LON).create();
      var sseListener = new WeakSSEListener();

      var nycServer = SERVERS.getTestServers().stream()
            .filter(testServer -> NYC.equals(testServer.getSiteName()))
            .findFirst();
      assertTrue(nycServer.isPresent());

      try (Closeable ignored = client.raw().listen("/rest/v2/container?action=listen&category=cross-site", Map.of(), sseListener)) {
         nycServer.get().getDriver().stop(0);
         sseListener.expectEvent("cross-site-event", "ISPN100018", SSEListener.NO_OP);

         nycServer.get().getDriver().restart(0);
         sseListener.expectEvent("cross-site-event", "ISPN100017", SSEListener.NO_OP);

         // no cluster events, this is new and is only a cross-site event
         sseListener.expectNoEvent("cluster-event", Assertions::fail);
      } finally {
         if (!nycServer.get().getDriver().isRunning(0)) {
            nycServer.get().getDriver().restart(0);
         }
      }
   }

   private void testCrossSiteEvents(String url, RestClient client, RestCacheClient cache, RestOperations.AcceptSerialization serialization, boolean cluster, boolean crossSite) throws IOException, InterruptedException {
      var sseListener = new WeakSSEListener();
      try (Closeable ignored = client.raw().listen(url, Map.of("Accept", serialization.header()), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));

         assertThat(cache.takeSiteOffline(NYC)).isOk();

         if (cluster) {
            sseListener.expectEvent("cluster-event", "ISPN100006", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("cluster-event", Assertions::fail);
         }

         if (crossSite) {
            sseListener.expectEvent("cross-site-event", "ISPN100006", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("cross-site-event", Assertions::fail);
         }

         assertThat(cache.bringSiteOnline(NYC)).isOk();

         if (cluster) {
            sseListener.expectEvent("cluster-event", "ISPN100005", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("cluster-event", Assertions::fail);
         }

         if (crossSite) {
            sseListener.expectEvent("cross-site-event", "ISPN100005", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("cross-site-event", Assertions::fail);
         }

         sseListener.expectNoEvent("create-cache", Assertions::fail);
         sseListener.expectNoEvent("remove-cache", Assertions::fail);
         sseListener.expectNoEvent("update-cache", Assertions::fail);
         sseListener.expectNoEvent("create-template", Assertions::fail);
         sseListener.expectNoEvent("remove-template", Assertions::fail);
         sseListener.expectNoEvent("update-template", Assertions::fail);
         sseListener.expectNoEvent("lifecycle-event", Assertions::fail);
         sseListener.expectNoEvent("tasks-event", Assertions::fail);
         sseListener.expectNoEvent("security-event", Assertions::fail);
      }
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
      eventuallyEquals("v1", () -> sync(nycCache.get("k1")).body());
      if (allSitesBackup) {
         eventuallyEquals("v2", () -> sync(lonCache.get("k2")).body());
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
      assertStatus(200, cache.createWithTemplate("org.infinispan.DIST_SYNC"));
      return cache;
   }

}
