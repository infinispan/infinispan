package org.infinispan.rest.resources;

import static org.infinispan.xsite.XSiteAdminOperations.OFFLINE;
import static org.infinispan.xsite.XSiteAdminOperations.ONLINE;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.ControlledTransport;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.Exceptions;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.xsite.statetransfer.AbstractStateTransferTest;
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
public class XSiteResourceTest extends AbstractStateTransferTest {
   private static final String CACHE = XSiteResourceTest.class.getSimpleName();

   private Map<String, RestServerHelper> restServerPerSite = new HashMap<>(2);
   private Map<String, RestClient> clientPerSite = new HashMap<>(2);
   private final ObjectMapper MAPPER = new ObjectMapper();

   public XSiteResourceTest() {
      initialClusterSize = 1;
      cacheMode = CacheMode.DIST_SYNC;
      implicitBackupCache = true;
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
      return restClient.cache(CACHE);
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
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(cacheMode, false);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(cacheMode, false);
   }

   @Test
   public void testObtainBackupStatus() throws Exception {
      assertEquals(ONLINE, getBackupStatus(LON));
      assertEquals(ONLINE, getBackupStatus(NYC));
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
      RestCacheClient cache = client.cache(CACHE);
      RestResponse response = sync(cache.backupStatus("invalid-site"));
      assertEquals(404, response.getStatus());
   }

   @Test
   public void testOnlineOffline() throws Exception {
      testOnlineOffline(LON);
      testOnlineOffline(NYC);
   }

   @Test
   public void testBackups() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestResponse response = sync(cache.xsiteBackups());
      assertEquals(200, response.getStatus());

      JsonNode status = MAPPER.readTree(response.getBody());
      assertEquals(ONLINE, status.get(NYC).asText());
   }

   @Test
   public void testPushState() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestCacheClient backupCache = getCacheClient(NYC);
      String key = "key";
      String value = "value";
      Function<String, Integer> keyOnBackup = k -> sync(backupCache.get(key)).getStatus();

      takeBackupOffline(LON);
      assertEquals(OFFLINE, getBackupStatus(LON));

      sync(cache.put(key, value));
      assertEquals(404, (int) keyOnBackup.apply(key));

      RestResponse response = sync(cache.pushSiteState(NYC));
      assertEquals(200, response.getStatus());

      eventually(() -> getBackupStatus(LON).equals(ONLINE));
      eventually(() -> keyOnBackup.apply(key) == 200);
   }

   @Test
   public void testCancelPushState() throws Exception {
      RestCacheClient cache = getCacheClient(LON);
      RestCacheClient backupCache = getCacheClient(NYC);

      // Take backup offline
      takeBackupOffline(LON);
      assertEquals(OFFLINE, getBackupStatus(LON));

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

      assertEquals("CANCELED", getPushStatus(LON));

      // Clear status
      response = sync(cache.clearPushStateStatus());
      assertEquals(200, response.getStatus());

      assertEquals("", getPushStatus(LON));

      response = sync(cache.cancelReceiveState(NYC));
      assertEquals(200, response.getStatus());
   }

   @Test
   public void testTakeOfflineConfig() throws Exception {
      RestCacheClient cacheClient = getCacheClient(LON);

      RestResponse response = sync(cacheClient.getXSiteTakeOfflineConfig(NYC));
      JsonNode takeOfflineConfig = MAPPER.readTree(response.getBody());

      assertEquals(0, takeOfflineConfig.get("afterFailures").asInt());
      assertEquals(0, takeOfflineConfig.get("minTimeToWait").asInt());

      response = sync(cacheClient.updateXSiteTakeOfflineConfig(NYC, 5, 1000));
      assertEquals(200, response.getStatus());

      response = sync(cacheClient.getXSiteTakeOfflineConfig(NYC));
      takeOfflineConfig = MAPPER.readTree(response.getBody());

      assertEquals(5, takeOfflineConfig.get("afterFailures").asInt());
      assertEquals(1000, takeOfflineConfig.get("minTimeToWait").asInt());
   }

   @Test
   public void testInvalidInputTakeOffline() {
      RestClient restClient = clientPerSite.get(LON);
      String url = String.format("/rest/v2/caches/%s/x-site/backups/%s/take-offline-config", CACHE, NYC);
      RestResponse response = sync(restClient.raw().putValue(url, new HashMap<>(), "invalid", "application/json"));
      assertEquals(400, response.getStatus());
   }

   private int getCacheSize(RestCacheClient cacheClient) {
      return Integer.parseInt(sync(cacheClient.size()).getBody());
   }

   private String getPushStatus(String site) throws Exception {
      RestCacheClient cache = getCacheClient(site);
      String backup = getBackup(site);
      JsonNode response = MAPPER.readTree(sync(cache.pushStateStatus()).getBody());
      if (response.isEmpty()) return "";
      return response.get(backup).asText();
   }

   private void testOnlineOffline(String site) throws Exception {
      takeBackupOffline(site);

      String siteStatus = getBackupStatus(site);
      assertEquals(siteStatus, OFFLINE);

      bringBackupOnline(site);

      siteStatus = getBackupStatus(site);
      assertEquals(siteStatus, ONLINE);
   }

   private void takeBackupOffline(String site) {
      RestCacheClient client = getCacheClient(site);
      String backup = site.equals(LON) ? NYC : LON;
      RestResponse response = sync(client.takeSiteOffline(backup));
      assertEquals(200, response.getStatus());
   }

   private void bringBackupOnline(String site) {
      RestCacheClient client = getCacheClient(site);
      RestResponse response = sync(client.bringSiteOnline(getBackup(site)));
      assertEquals(200, response.getStatus());
   }

   private String getFirstCacheManagerAddress(String site) {
      TestSite testSite = sites.stream().filter(t -> t.getSiteName().equals(site)).findFirst().orElse(null);
      if (testSite == null) return null;
      EmbeddedCacheManager cacheManager = testSite.cacheManagers().iterator().next();
      return cacheManager.getAddress().toString();
   }

   private String getBackupStatus(String site) throws Exception {
      RestCacheClient cacheClient = getCacheClient(site);
      RestResponse response = sync(cacheClient.backupStatus(getBackup(site)));
      assertEquals(200, response.getStatus());

      JsonNode json = MAPPER.readTree(response.getBody());
      String cacheManagerAddress = getFirstCacheManagerAddress(site);
      return json.get(cacheManagerAddress).asText();
   }

   public static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
   }

   private String getBackup(String site) {
      if (site.equals(LON)) return NYC;
      if (site.equals(NYC)) return LON;
      throw new IllegalArgumentException("Invalid site");
   }

}
