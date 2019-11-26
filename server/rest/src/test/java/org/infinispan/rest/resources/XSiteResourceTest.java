package org.infinispan.rest.resources;

import static org.infinispan.xsite.XSiteAdminOperations.OFFLINE;
import static org.infinispan.xsite.XSiteAdminOperations.ONLINE;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.ControlledTransport;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.Exceptions;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
   private final ObjectMapper MAPPER = new ObjectMapper();

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
         restServerHelper.start(TestResourceTracker.getCurrentTestShortName());
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

   @AfterClass
   public void clean() {
      restServerPerSite.values().forEach(RestServerHelper::stop);
      clientPerSite.values().forEach(cli -> {
         try {
            cli.close();
         } catch (IOException ignored) {
         }
      });
   }

   @AfterMethod
   public void cleanCache() {
      sync(getCacheClient(LON).clear());
      sync(getCacheClient(NYC).clear());
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Test
   public void testObtainBackupStatus() throws Exception {
      assertEquals(ONLINE, getBackupStatus(LON, NYC));
      assertEquals(ONLINE, getBackupStatus(LON, SFO));
      assertEquals(ONLINE, getBackupStatus(NYC, LON));
      assertEquals(ONLINE, getBackupStatus(NYC, SFO));
   }

   @Test
   public void testInvalidCache() {
      RestClient client = clientPerSite.get(LON);
      RestResponse response = sync(client.cache("invalid-cache").xsiteBackups());
      assertEquals(404, response.getStatus());
   }

   @Test
   public void testInvalidSite() {
      RestClient client = clientPerSite.get(LON);
      RestCacheClient cache = client.cache(CACHE_1);
      RestResponse response = sync(cache.backupStatus("invalid-site"));
      assertEquals(404, response.getStatus());
   }

   @Test
   public void testOnlineOffline() throws Exception {
      testOnlineOffline(LON, NYC);
      testOnlineOffline(NYC, LON);
   }

   @Test
   public void testBackups() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestResponse response = sync(cache.xsiteBackups());
      assertEquals(200, response.getStatus());

      JsonNode status = MAPPER.readTree(response.getBody().toString());
      assertEquals(ONLINE, status.get(NYC).asText());
   }

   @Test
   public void testPushState() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestCacheClient backupCache = getCacheClient(NYC);
      String key = "key";
      String value = "value";
      Function<String, Integer> keyOnBackup = k -> sync(backupCache.get(key)).getStatus();

      takeBackupOffline(LON, NYC);
      assertEquals(OFFLINE, getBackupStatus(LON, NYC));
      assertEquals(ONLINE, getBackupStatus(LON, SFO));

      sync(cache.put(key, value));
      assertEquals(404, (int) keyOnBackup.apply(key));

      RestResponse response = sync(cache.pushSiteState(NYC));
      assertEquals(200, response.getStatus());

      eventually(() -> getBackupStatus(LON, NYC).equals(ONLINE));
      eventually(() -> keyOnBackup.apply(key) == 200);
   }

   @Test
   public void testCancelPushState() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestCacheClient backupCache = getCacheClient(NYC);

      // Take backup offline
      takeBackupOffline(LON, NYC);
      assertEquals(OFFLINE, getBackupStatus(LON, NYC));

      // Write in the cache
      int entries = 500;
      IntStream.range(0, entries).forEach(i -> sync(cache.put(String.valueOf(i), "value")));

      // Backup should be empty
      assertEquals(entries, getCacheSize(cache));
      assertEquals(0, getCacheSize(backupCache));

      // Start state push
      ControlledTransport controllerTransport = ControlledTransport.replace(cache(LON, 0));
      controllerTransport.blockBefore(XSiteStatePushCommand.class);
      sync(cache.pushSiteState(NYC));
      controllerTransport.waitForCommandToBlock();

      // Cancel push
      RestResponse response = sync(cache.cancelPushState(NYC));
      assertEquals(200, response.getStatus());

      controllerTransport.stopBlocking();

      JsonNode status = MAPPER.readTree(sync(cache.pushStateStatus()).getBody());
      assertEquals("CANCELED", status.get(NYC).asText());

      // Clear status
      response = sync(cache.clearPushStateStatus());
      assertEquals(200, response.getStatus());

      status = MAPPER.readTree(sync(cache.pushStateStatus()).getBody());
      assertTrue(status.isEmpty());

      response = sync(cache.cancelReceiveState(NYC));
      assertEquals(200, response.getStatus());
   }

   @Test
   public void testTakeOfflineConfig() throws Exception {
      RestCacheClient cacheClient = getCacheClient(LON);

      RestResponse response = sync(cacheClient.getXSiteTakeOfflineConfig(NYC));
      JsonNode takeOfflineConfig = MAPPER.readTree(response.getBody());

      assertEquals(0, takeOfflineConfig.get("after_failures").asInt());
      assertEquals(0, takeOfflineConfig.get("min_wait").asInt());

      response = sync(cacheClient.updateXSiteTakeOfflineConfig(NYC, 5, 1000));
      assertEquals(204, response.getStatus());

      response = sync(cacheClient.getXSiteTakeOfflineConfig(NYC));
      takeOfflineConfig = MAPPER.readTree(response.getBody());

      assertEquals(5, takeOfflineConfig.get("after_failures").asInt());
      assertEquals(1000, takeOfflineConfig.get("min_wait").asInt());
   }

   @Test
   public void testInvalidInputTakeOffline() {
      RestClient restClient = clientPerSite.get(LON);
      String url = String.format("/rest/v2/caches/%s/x-site/backups/%s/take-offline-config", CACHE_1, NYC);
      RestResponse response = sync(restClient.raw().putValue(url, new HashMap<>(), "invalid", "application/json"));
      assertEquals(400, response.getStatus());
   }

   @Test
   public void testGetStatusAllCaches() throws Exception {
      RestClient restClient = clientPerSite.get(LON);

      assertAllSitesOnline(restClient);

      sync(restClient.cache(CACHE_2).takeSiteOffline(NYC));
      RestResponse response = sync(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      JsonNode json = MAPPER.readTree(response.getBody());
      assertEquals(json.get(NYC).get("status").asText(), "mixed");
      assertEquals(json.get(NYC).get("online").elements().next().asText(), CACHE_1);
      assertEquals(json.get(NYC).get("offline").elements().next().asText(), CACHE_2);
      sync(restClient.cache(CACHE_2).bringSiteOnline(NYC));

      assertAllSitesOnline(restClient);
   }


   @Test
   public void testBringAllCachesOnlineOffline() throws Exception {
      RestClient restClient = clientPerSite.get(LON);
      RestCacheManagerClient restCacheManagerClient = restClient.cacheManager(CACHE_MANAGER);

      RestResponse response = sync(restCacheManagerClient.takeOffline(SFO));
      assertEquals(200, response.getStatus());

      response = sync(restCacheManagerClient.backupStatuses());
      JsonNode json = MAPPER.readTree(response.getBody());
      assertEquals(json.get(SFO).get("status").asText(), "offline");

      sync(restCacheManagerClient.bringBackupOnline(SFO));
      response = sync(restCacheManagerClient.backupStatuses());
      json = MAPPER.readTree(response.getBody());
      assertEquals(json.get(SFO).get("status").asText(), "online");
   }

   @Test
   public void testPushAllCaches() throws Exception {
      RestClient restClientLon = clientPerSite.get(LON);
      RestClient restClientSfo = clientPerSite.get(SFO);

      RestCacheClient cache1Lon = restClientLon.cache(CACHE_1);
      RestCacheClient cache2Lon = restClientLon.cache(CACHE_2);

      RestCacheClient cache1Sfo = restClientSfo.cache(CACHE_1);
      RestCacheClient cache2Sfo = restClientSfo.cache(CACHE_2);

      // Take SFO offline for all caches
      sync(restClientLon.cacheManager(CACHE_MANAGER).takeOffline(SFO));
      RestResponse statusResponse = sync(restClientLon.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals("offline", MAPPER.readTree(statusResponse.getBody()).get(SFO).get("status").asText());

      // Write to the caches
      int entries = 10;
      IntStream.range(0, entries).forEach(i -> {
         String key = String.valueOf(i);
         String value = "value";
         sync(cache1Lon.put(key, value));
         sync(cache2Lon.put(key, value));
      });

      // Backups should be empty
      assertEquals(0, getCacheSize(cache1Sfo));
      assertEquals(0, getCacheSize(cache2Sfo));

      // Start state push
      RestResponse response = sync(restClientLon.cacheManager(CACHE_MANAGER).pushSiteState(SFO));
      assertEquals(200, response.getStatus());

      // Backups should eventually be online...
      eventually(() -> {
         RestResponse restResponse = sync(restClientLon.cacheManager(CACHE_MANAGER).backupStatuses());
         String status = MAPPER.readTree(restResponse.getBody()).get(SFO).get("status").asText();
         return "online".equals(status);
      });

      // ... and with state
      assertEquals(entries, getCacheSize(cache1Sfo));
      assertEquals(entries, getCacheSize(cache2Sfo));
   }

   @Test
   public void testCancelPushAllCaches() throws Exception {
      RestClient restClientLon = clientPerSite.get(LON);
      RestCacheClient cache1Lon = restClientLon.cache(CACHE_1);
      RestCacheClient cache2Lon = restClientLon.cache(CACHE_2);
      sync(cache1Lon.put("k1", "v1"));
      sync(cache2Lon.put("k2", "v2"));

      // Block before pushing state on both caches
      ControlledTransport controlledTransport = ControlledTransport.replace(cache(LON, CACHE_1, 0));
      controlledTransport.blockBefore(XSiteStatePushCommand.class);

      // Trigger a state push
      sync(restClientLon.cacheManager(CACHE_MANAGER).pushSiteState(SFO));
      controlledTransport.waitForCommandToBlock();

      // Cancel state push
      RestResponse response = sync(restClientLon.cacheManager(CACHE_MANAGER).cancelPushState(SFO));
      assertEquals(200, response.getStatus());
      controlledTransport.stopBlocking();

      // Verify that push was cancelled for both caches
      JsonNode pushStatusCache1 = MAPPER.readTree(sync(cache1Lon.pushStateStatus()).getBody());
      JsonNode pushStatusCache2 = MAPPER.readTree(sync(cache2Lon.pushStateStatus()).getBody());

      assertEquals("CANCELED", pushStatusCache1.get(SFO).asText());
      assertEquals("CANCELED", pushStatusCache2.get(SFO).asText());
   }

   private int getCacheSize(RestCacheClient cacheClient) {
      return Integer.parseInt(sync(cacheClient.size()).getBody());
   }

   private void testOnlineOffline(String site, String backup) throws Exception {
      takeBackupOffline(site, backup);

      String siteStatus = getBackupStatus(site, backup);
      assertEquals(siteStatus, OFFLINE);

      bringBackupOnline(site, backup);

      siteStatus = getBackupStatus(site, backup);
      assertEquals(siteStatus, ONLINE);
   }

   private void takeBackupOffline(String site, String backup) {
      RestCacheClient client = getCacheClient(site);
      RestResponse response = sync(client.takeSiteOffline(backup));
      assertEquals(200, response.getStatus());
   }

   private void bringBackupOnline(String site, String backup) {
      RestCacheClient client = getCacheClient(site);
      RestResponse response = sync(client.bringSiteOnline(backup));
      assertEquals(200, response.getStatus());
   }

   private String getFirstCacheManagerAddress(String site) {
      TestSite testSite = sites.stream().filter(t -> t.getSiteName().equals(site)).findFirst().orElse(null);
      if (testSite == null) return null;
      EmbeddedCacheManager cacheManager = testSite.cacheManagers().iterator().next();
      return cacheManager.getAddress().toString();
   }

   private String getBackupStatus(String site, String backup) throws Exception {
      RestCacheClient cacheClient = getCacheClient(site);
      RestResponse response = sync(cacheClient.backupStatus(backup));
      assertEquals(200, response.getStatus());

      JsonNode json = MAPPER.readTree(response.getBody());
      String cacheManagerAddress = getFirstCacheManagerAddress(site);
      return json.get(cacheManagerAddress).asText();
   }

   private void assertAllSitesOnline(RestClient restClient, String... sites) throws Exception {
      RestResponse response = sync(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals(200, response.getStatus());
      JsonNode json = MAPPER.readTree(response.getBody());
      Arrays.stream(sites).forEach(s -> assertEquals(json.get(s).get("status").asText(), "online"));
   }

   public static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
   }

   @Override
   protected void afterSitesCreated() {
      // LON backs-up to SFO, NYC
      ConfigurationBuilder builder = defaultConfigurationForSite(0);
      builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
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
      builder.sites().addBackup().site(siteName(0)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC);
      defineInSite(site(1), CACHE_1, builder.build());
      defineInSite(site(1), CACHE_2, builder.build());
      site(1).waitForClusterToForm(CACHE_1);
      site(1).waitForClusterToForm(CACHE_2);
   }
}
