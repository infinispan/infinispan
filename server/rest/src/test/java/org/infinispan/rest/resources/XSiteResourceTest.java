package org.infinispan.rest.resources;

import static org.infinispan.rest.helper.RestResponses.assertNoContent;
import static org.infinispan.rest.helper.RestResponses.assertStatus;
import static org.infinispan.rest.helper.RestResponses.assertSuccessful;
import static org.infinispan.rest.helper.RestResponses.jsonResponseBody;
import static org.infinispan.rest.helper.RestResponses.responseBody;
import static org.infinispan.rest.helper.RestResponses.responseStatus;
import static org.infinispan.xsite.XSiteAdminOperations.OFFLINE;
import static org.infinispan.xsite.XSiteAdminOperations.ONLINE;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.ControlledTransport;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * @since 10.0
 */
@Test(groups = "xsite", testName = "rest.XSiteResourceTest")
public class XSiteResourceTest extends AbstractMultipleSitesTest {
   private static final String LON = "LON-1";
   private static final String NYC = "NYC-2";
   private static final String SFO = "SFO-3";

   private static final String CACHE_1 = "CACHE_1";
   private static final String CACHE_2 = "CACHE_2";
   private static final String CACHE_MANAGER = "default";

   private Map<String, RestServerHelper> restServerPerSite = new HashMap<>(2);
   private Map<String, RestClient> clientPerSite = new HashMap<>(2);

   protected int defaultNumberOfSites() {
      return 3;
   }

   /**
    * @return the number of nodes per site.
    */
   protected int defaultNumberOfNodes() {
      return 1;
   }

   @BeforeClass
   public void startServers() {
      sites.forEach(site -> {
         String siteName = site.getSiteName();
         EmbeddedCacheManager cm = site.cacheManagers().iterator().next();
         RestServerHelper restServerHelper = new RestServerHelper(cm);
         restServerHelper.start(TestResourceTracker.getCurrentTestShortName() + "-" + cm.getAddress());
         restServerPerSite.put(siteName, restServerHelper);
         RestClientConfiguration clientConfig = new RestClientConfigurationBuilder()
               .addServer().host("127.0.0.1")
               .port(restServerHelper.getPort())
               .build();
         RestClient client = RestClient.forConfiguration(clientConfig);
         clientPerSite.put(siteName, client);
      });
   }

   private RestCacheClient getCacheClient(String site) {
      RestClient restClient = clientPerSite.get(site);
      return restClient.cache(CACHE_1);
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder configurationBuilder = super.defaultGlobalConfigurationForSite(siteIndex);
      configurationBuilder.cacheManagerName("default");
      return configurationBuilder;
   }

   @AfterClass(alwaysRun = true)
   public void clean() {
      restServerPerSite.values().forEach(RestServerHelper::stop);
      clientPerSite.values().forEach(cli -> {
         try {
            cli.close();
         } catch (IOException ignored) {
         }
      });
   }

   @AfterMethod(alwaysRun = true)
   public void cleanCache() {
      assertNoContent(getCacheClient(LON).clear());
      assertNoContent(getCacheClient(NYC).clear());
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Test
   public void testObtainBackupStatus() {
      assertEquals(ONLINE, getBackupStatus(LON, NYC));
      assertEquals(ONLINE, getBackupStatus(LON, SFO));
      assertEquals(ONLINE, getBackupStatus(NYC, LON));
      assertEquals(ONLINE, getBackupStatus(NYC, SFO));
   }

   @Test
   public void testInvalidCache() {
      RestClient client = clientPerSite.get(LON);
      assertStatus(404, client.cache("invalid-cache").xsiteBackups());
   }

   @Test
   public void testInvalidSite() {
      RestClient client = clientPerSite.get(LON);
      RestCacheClient cache = client.cache(CACHE_1);
      assertStatus(404, cache.backupStatus("invalid-site"));
   }

   @Test
   public void testOnlineOffline() {
      testOnlineOffline(LON, NYC);
      testOnlineOffline(NYC, LON);
   }

   @Test
   public void testBackups() {
      RestCacheClient cache = getCacheClient(LON);

      JsonNode status = jsonResponseBody(cache.xsiteBackups());
      assertEquals(ONLINE, status.get(NYC).asText());
   }

   @Test
   public void testPushState() {
      RestCacheClient cache = getCacheClient(LON);
      RestCacheClient backupCache = getCacheClient(NYC);
      String key = "key";
      String value = "value";
      Function<String, Integer> keyOnBackup = k -> responseStatus(backupCache.get(key));

      takeBackupOffline(LON, NYC);
      assertEquals(OFFLINE, getBackupStatus(LON, NYC));
      assertEquals(ONLINE, getBackupStatus(LON, SFO));

      assertNoContent(cache.put(key, value));
      assertEquals(404, (int) keyOnBackup.apply(key));

      assertSuccessful(cache.pushSiteState(NYC));
      assertEquals(ONLINE, getBackupStatus(LON, NYC));
      eventuallyEquals("OK", () -> pushStateStatus(cache, NYC));
      assertEquals(200, responseStatus(backupCache.get(key)));
   }

   @Test
   public void testCancelPushState() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestCacheClient backupCache = getCacheClient(NYC);

      // Take backup offline
      takeBackupOffline(LON, NYC);
      assertEquals(OFFLINE, getBackupStatus(LON, NYC));

      // Write in the cache
      int entries = 50;
      IntStream.range(0, entries).forEach(i -> assertNoContent(cache.put(String.valueOf(i), "value")));

      // Backup should be empty
      assertEquals(entries, getCacheSize(cache));
      assertEquals(0, getCacheSize(backupCache));

      // Start state push
      ControlledTransport controllerTransport = ControlledTransport.replace(cache(LON, 0));
      controllerTransport.blockBefore(XSiteStatePushCommand.class);
      assertSuccessful(cache.pushSiteState(NYC));
      controllerTransport.waitForCommandToBlock();

      // Cancel push
      assertSuccessful(cache.cancelPushState(NYC));

      controllerTransport.stopBlocking();

      JsonNode status = jsonResponseBody(cache.pushStateStatus());
      assertEquals("CANCELED", status.get(NYC).asText());

      // Clear status
      assertSuccessful(cache.clearPushStateStatus());

      status = jsonResponseBody(cache.pushStateStatus());
      assertTrue(status.isEmpty());

      assertSuccessful(cache.cancelReceiveState(NYC));
   }

   @Test
   public void testTakeOfflineConfig() {
      RestCacheClient cacheClient = getCacheClient(LON);

      JsonNode takeOfflineConfig = jsonResponseBody(cacheClient.getXSiteTakeOfflineConfig(NYC));

      assertEquals(0, takeOfflineConfig.get("after_failures").asInt());
      assertEquals(0, takeOfflineConfig.get("min_wait").asInt());

      assertNoContent(cacheClient.updateXSiteTakeOfflineConfig(NYC, 5, 1000));

      takeOfflineConfig = jsonResponseBody(cacheClient.getXSiteTakeOfflineConfig(NYC));

      assertEquals(5, takeOfflineConfig.get("after_failures").asInt());
      assertEquals(1000, takeOfflineConfig.get("min_wait").asInt());
   }

   @Test
   public void testInvalidInputTakeOffline() {
      RestClient restClient = clientPerSite.get(LON);
      String url = String.format("/rest/v2/caches/%s/x-site/backups/%s/take-offline-config", CACHE_1, NYC);
      assertStatus(400, restClient.raw().putValue(url, new HashMap<>(), "invalid", "application/json"));
   }

   @Test
   public void testGetStatusAllCaches() {
      RestClient restClient = clientPerSite.get(LON);

      assertAllSitesOnline(restClient);

      assertSuccessful(restClient.cache(CACHE_2).takeSiteOffline(NYC));

      JsonNode json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals(json.get(NYC).get("status").asText(), "mixed");
      assertEquals(json.get(NYC).get("online").elements().next().asText(), CACHE_1);
      assertEquals(json.get(NYC).get("offline").elements().next().asText(), CACHE_2);

      assertSuccessful(restClient.cache(CACHE_2).bringSiteOnline(NYC));

      assertAllSitesOnline(restClient);
   }


   @Test
   public void testBringAllCachesOnlineOffline() {
      RestClient restClient = clientPerSite.get(LON);
      RestCacheManagerClient restCacheManagerClient = restClient.cacheManager(CACHE_MANAGER);

      assertSuccessful(restCacheManagerClient.takeOffline(SFO));

      JsonNode json = jsonResponseBody(restCacheManagerClient.backupStatuses());
      assertEquals(json.get(SFO).get("status").asText(), "offline");

      assertSuccessful(restCacheManagerClient.bringBackupOnline(SFO));

      json = jsonResponseBody(restCacheManagerClient.backupStatuses());
      assertEquals(json.get(SFO).get("status").asText(), "online");
   }

   @Test
   public void testPushAllCaches() {
      RestClient restClientLon = clientPerSite.get(LON);
      RestClient restClientSfo = clientPerSite.get(SFO);

      RestCacheClient cache1Lon = restClientLon.cache(CACHE_1);
      RestCacheClient cache2Lon = restClientLon.cache(CACHE_2);

      RestCacheClient cache1Sfo = restClientSfo.cache(CACHE_1);
      RestCacheClient cache2Sfo = restClientSfo.cache(CACHE_2);

      // Take SFO offline for all caches
      assertSuccessful(restClientLon.cacheManager(CACHE_MANAGER).takeOffline(SFO));
      JsonNode backupStatuses = jsonResponseBody(restClientLon.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals("offline", backupStatuses.get(SFO).get("status").asText());

      // Write to the caches
      int entries = 10;
      IntStream.range(0, entries).forEach(i -> {
         String key = String.valueOf(i);
         String value = "value";
         assertNoContent(cache1Lon.put(key, value));
         assertNoContent(cache2Lon.put(key, value));
      });

      // Backups should be empty
      assertEquals(0, getCacheSize(cache1Sfo));
      assertEquals(0, getCacheSize(cache2Sfo));

      // Start state push
      assertSuccessful(restClientLon.cacheManager(CACHE_MANAGER).pushSiteState(SFO));

      // Backups go online online immediately
      assertEquals(ONLINE, getBackupStatus(LON, SFO));

      // State push should eventually finish
      eventuallyEquals("OK", () -> pushStateStatus(cache1Lon, SFO));
      eventuallyEquals("OK", () -> pushStateStatus(cache2Lon, SFO));

      // ... and with state
      assertEquals(entries, getCacheSize(cache1Sfo));
      assertEquals(entries, getCacheSize(cache2Sfo));
   }

   private String pushStateStatus(RestCacheClient cacheClient, String siteName) {
      JsonNode json = jsonResponseBody(cacheClient.pushStateStatus());
      return json.get(siteName).asText();
   }

   @Test
   public void testCancelPushAllCaches() throws Exception {
      RestClient restClientLon = clientPerSite.get(LON);
      RestCacheClient cache1Lon = restClientLon.cache(CACHE_1);
      RestCacheClient cache2Lon = restClientLon.cache(CACHE_2);
      assertNoContent(cache1Lon.put("k1", "v1"));
      assertNoContent(cache2Lon.put("k2", "v2"));

      // Block before pushing state on both caches
      ControlledTransport controlledTransport = ControlledTransport.replace(cache(LON, CACHE_1, 0));
      controlledTransport.blockBefore(XSiteStatePushCommand.class);

      // Trigger a state push
      assertSuccessful(restClientLon.cacheManager(CACHE_MANAGER).pushSiteState(SFO));
      controlledTransport.waitForCommandToBlock();

      // Cancel state push
      assertSuccessful(restClientLon.cacheManager(CACHE_MANAGER).cancelPushState(SFO));
      controlledTransport.stopBlocking();

      // Verify that push was cancelled for both caches
      JsonNode pushStatusCache1 = jsonResponseBody(cache1Lon.pushStateStatus());
      JsonNode pushStatusCache2 = jsonResponseBody(cache2Lon.pushStateStatus());

      assertEquals("CANCELED", pushStatusCache1.get(SFO).asText());
      assertEquals("CANCELED", pushStatusCache2.get(SFO).asText());
   }

   private int getCacheSize(RestCacheClient cacheClient) {
      return Integer.parseInt(responseBody(cacheClient.size()));
   }

   private void testOnlineOffline(String site, String backup) {
      takeBackupOffline(site, backup);

      String siteStatus = getBackupStatus(site, backup);
      assertEquals(siteStatus, OFFLINE);

      bringBackupOnline(site, backup);

      siteStatus = getBackupStatus(site, backup);
      assertEquals(siteStatus, ONLINE);
   }

   private void takeBackupOffline(String site, String backup) {
      RestCacheClient client = getCacheClient(site);
      assertSuccessful(client.takeSiteOffline(backup));
   }

   private void bringBackupOnline(String site, String backup) {
      RestCacheClient client = getCacheClient(site);
      assertSuccessful(client.bringSiteOnline(backup));
   }

   private String getFirstCacheManagerAddress(String site) {
      TestSite testSite = sites.stream().filter(t -> t.getSiteName().equals(site)).findFirst().orElse(null);
      if (testSite == null) return null;
      EmbeddedCacheManager cacheManager = testSite.cacheManagers().iterator().next();
      return cacheManager.getAddress().toString();
   }

   private String getBackupStatus(String site, String backup) {
      RestCacheClient cacheClient = getCacheClient(site);
      String cacheManagerAddress = getFirstCacheManagerAddress(site);

      JsonNode json = jsonResponseBody(cacheClient.backupStatus(backup));
      return json.get(cacheManagerAddress).asText();
   }

   private void assertAllSitesOnline(RestClient restClient, String... sites) {
      JsonNode json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      Arrays.stream(sites).forEach(s -> assertEquals(json.get(s).get("status").asText(), "online"));
   }

   @Override
   protected void afterSitesCreated() {
      // LON backs-up to SFO, NYC
      ConfigurationBuilder builder = defaultConfigurationForSite(0);
      builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC)
             .stateTransfer().chunkSize(5);
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC)
             .stateTransfer().chunkSize(5);
      defineInSite(site(0), CACHE_1, builder.build());
      defineInSite(site(0), CACHE_2, builder.build());
      defineInSite(site(2), CACHE_1, builder.build());
      defineInSite(site(2), CACHE_2, builder.build());
      site(0).waitForClusterToForm(CACHE_1);
      site(0).waitForClusterToForm(CACHE_2);
      site(2).waitForClusterToForm(CACHE_1);
      site(2).waitForClusterToForm(CACHE_2);

      // NYC backs up to LON, SFO
      builder = defaultConfigurationForSite(1);
      builder.sites().addBackup().site(siteName(0)).strategy(BackupConfiguration.BackupStrategy.SYNC)
             .stateTransfer().chunkSize(5);
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC)
             .stateTransfer().chunkSize(5);
      defineInSite(site(1), CACHE_1, builder.build());
      defineInSite(site(1), CACHE_2, builder.build());
      site(1).waitForClusterToForm(CACHE_1);
      site(1).waitForClusterToForm(CACHE_2);
   }
}
