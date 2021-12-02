package org.infinispan.rest.resources;

import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;
import static org.infinispan.rest.helper.RestResponses.assertNoContent;
import static org.infinispan.rest.helper.RestResponses.assertStatus;
import static org.infinispan.rest.helper.RestResponses.assertSuccessful;
import static org.infinispan.rest.helper.RestResponses.jsonResponseBody;
import static org.infinispan.rest.helper.RestResponses.responseBody;
import static org.infinispan.rest.helper.RestResponses.responseStatus;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.infinispan.xsite.XSiteAdminOperations.OFFLINE;
import static org.infinispan.xsite.XSiteAdminOperations.ONLINE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.AbstractDelegatingTransport;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.XSiteResponse;
import org.infinispan.remoting.transport.impl.XSiteResponseImpl;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import net.jcip.annotations.GuardedBy;

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

   private final Map<String, RestServerHelper> restServerPerSite = new HashMap<>(2);
   private final Map<String, RestClient> clientPerSite = new HashMap<>(2);

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

   @AfterClass(alwaysRun = true)
   public void clean() {
      clientPerSite.values().forEach(cli -> {
         try {
            cli.close();
         } catch (IOException ignored) {
         }
      });
      restServerPerSite.values().forEach(RestServerHelper::stop);
   }

   @AfterMethod(alwaysRun = true)
   public void cleanCache() {
      while (site(LON).cacheManagers().size() > 1) {
         site(LON).kill(1);
         site(LON).waitForClusterToForm(CACHE_1);
         site(LON).waitForClusterToForm(CACHE_2);
      }
      assertNoContent(getCacheClient(LON).clear());
      assertNoContent(getCacheClient(NYC).clear());
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numSegments(2);
      return builder;
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

      Json status = jsonResponseBody(cache.xsiteBackups());
      assertEquals(ONLINE, status.at(NYC).at("status").asString());

      // add a second node
      TestSite site = site(LON);
      EmbeddedCacheManager cm = site.addCacheManager(null, defaultGlobalConfigurationForSite(site.getSiteIndex()), defaultConfigurationForSite(site.getSiteIndex()), false);
      site.waitForClusterToForm(CACHE_1);
      site.waitForClusterToForm(CACHE_2);

      TakeOfflineManager takeOfflineManager = TestingUtil.extractComponent(cm.getCache(CACHE_1), TakeOfflineManager.class);
      takeOfflineManager.takeSiteOffline(NYC);

      String node1 = String.valueOf(site.cacheManagers().get(0).getAddress());
      String node2 = String.valueOf(cm.getAddress());

      status = jsonResponseBody(cache.xsiteBackups());
      assertEquals("mixed", status.at(NYC).at("status").asString());
      assertEquals(status.at(NYC).at("online").asJsonList().iterator().next().asString(), node1);
      assertEquals(status.at(NYC).at("offline").asJsonList().iterator().next().asString(), node2);
      assertFalse(status.at(NYC).has("mixed"));

      status = jsonResponseBody(cache.backupStatus(NYC));
      assertEquals(ONLINE, status.at(node1).asString());
      assertEquals(OFFLINE, status.at(node2).asString());

      // bring NYC online
      takeOfflineManager.bringSiteOnline(NYC);

      status = jsonResponseBody(cache.xsiteBackups());
      assertEquals(ONLINE, status.at(NYC).at("status").asString());

      status = jsonResponseBody(cache.backupStatus(NYC));
      assertEquals(ONLINE, status.at(node1).asString());
      assertEquals(ONLINE, status.at(node2).asString());
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
      BlockXSitePushStateTransport transport = BlockXSitePushStateTransport.replace(cache(LON, 0));
      transport.startBlocking();
      assertSuccessful(cache.pushSiteState(NYC));
      transport.waitForCommand();

      // Cancel push
      assertSuccessful(cache.cancelPushState(NYC));

      transport.stopBlocking();

      Json status = jsonResponseBody(cache.pushStateStatus());
      assertEquals("CANCELED", status.at(NYC).asString());

      // Clear status
      assertSuccessful(cache.clearPushStateStatus());

      status = jsonResponseBody(cache.pushStateStatus());
      assertEquals(2, status.asMap().size());
      assertEquals("IDLE", status.asMap().get(NYC));
      assertEquals("IDLE", status.asMap().get(SFO));

      assertSuccessful(cache.cancelReceiveState(NYC));
   }

   @Test
   public void testTakeOfflineConfig() {
      RestCacheClient cacheClient = getCacheClient(LON);

      Json takeOfflineConfig = jsonResponseBody(cacheClient.getXSiteTakeOfflineConfig(NYC));

      assertEquals(0, takeOfflineConfig.at("after_failures").asInteger());
      assertEquals(0, takeOfflineConfig.at("min_wait").asInteger());

      assertNoContent(cacheClient.updateXSiteTakeOfflineConfig(NYC, 5, 1000));

      takeOfflineConfig = jsonResponseBody(cacheClient.getXSiteTakeOfflineConfig(NYC));

      assertEquals(5, takeOfflineConfig.at("after_failures").asInteger());
      assertEquals(1000, takeOfflineConfig.at("min_wait").asInteger());
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

      Json json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals(json.at(NYC).at("status").asString(), "mixed");
      assertEquals(json.at(NYC).at("online").asJsonList().iterator().next().asString(), CACHE_1);
      assertEquals(json.at(NYC).at("offline").asJsonList().iterator().next().asString(), CACHE_2);

      json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatus(NYC));
      assertEquals(json.at("status").asString(), "mixed");
      assertEquals(json.at("online").asJsonList().iterator().next().asString(), CACHE_1);
      assertEquals(json.at("offline").asJsonList().iterator().next().asString(), CACHE_2);

      assertSuccessful(restClient.cache(CACHE_2).bringSiteOnline(NYC));

      assertAllSitesOnline(restClient);

      // add a second node
      TestSite site = site(LON);
      EmbeddedCacheManager cm = site.addCacheManager(null, defaultGlobalConfigurationForSite(site.getSiteIndex()), defaultConfigurationForSite(site.getSiteIndex()), true);
      site.waitForClusterToForm(CACHE_1);
      site.waitForClusterToForm(CACHE_2);

      TakeOfflineManager takeOfflineManager = TestingUtil.extractComponent(cm.getCache(CACHE_1), TakeOfflineManager.class);
      takeOfflineManager.takeSiteOffline(NYC);

      json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals(json.at(NYC).at("status").asString(), "mixed");
      assertEquals(json.at(NYC).at("online").asJsonList().iterator().next().asString(), CACHE_2);
      assertTrue(json.at(NYC).at("offline").asJsonList().isEmpty());
      assertEquals(json.at(NYC).at("mixed").asJsonList().iterator().next().asString(), CACHE_1);

      json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatus(NYC));
      assertEquals(json.at("status").asString(), "mixed");
      assertEquals(json.at("online").asJsonList().iterator().next().asString(), CACHE_2);
      assertTrue(json.at("offline").asJsonList().isEmpty());
      assertEquals(json.at("mixed").asJsonList().iterator().next().asString(), CACHE_1);

      takeOfflineManager.bringSiteOnline(NYC);
   }


   @Test
   public void testBringAllCachesOnlineOffline() {
      RestClient restClient = clientPerSite.get(LON);
      RestCacheManagerClient restCacheManagerClient = restClient.cacheManager(CACHE_MANAGER);

      assertSuccessful(restCacheManagerClient.takeOffline(SFO));

      Json json = jsonResponseBody(restCacheManagerClient.backupStatuses());
      assertEquals(json.at(SFO).at("status").asString(), "offline");

      assertSuccessful(restCacheManagerClient.bringBackupOnline(SFO));

      json = jsonResponseBody(restCacheManagerClient.backupStatuses());
      assertEquals(json.at(SFO).at("status").asString(), "online");
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
      Json backupStatuses = jsonResponseBody(restClientLon.cacheManager(CACHE_MANAGER).backupStatuses());
      assertEquals("offline", backupStatuses.at(SFO).at("status").asString());

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
      Json json = jsonResponseBody(cacheClient.pushStateStatus());
      return json.at(siteName).asString();
   }

   @Test
   public void testCancelPushAllCaches() throws Exception {
      RestClient restClientLon = clientPerSite.get(LON);
      RestCacheClient cache1Lon = restClientLon.cache(CACHE_1);
      RestCacheClient cache2Lon = restClientLon.cache(CACHE_2);
      assertNoContent(cache1Lon.put("k1", "v1"));
      assertNoContent(cache2Lon.put("k2", "v2"));

      // Block before pushing state on both caches
      BlockXSitePushStateTransport transport = BlockXSitePushStateTransport.replace(cache(LON, CACHE_1, 0));
      transport.startBlocking();

      // Trigger a state push
      assertSuccessful(restClientLon.cacheManager(CACHE_MANAGER).pushSiteState(SFO));
      transport.waitForCommand();

      // Cancel state push
      assertSuccessful(restClientLon.cacheManager(CACHE_MANAGER).cancelPushState(SFO));
      transport.stopBlocking();

      // Verify that push was cancelled for both caches
      Json pushStatusCache1 = jsonResponseBody(cache1Lon.pushStateStatus());
      Json pushStatusCache2 = jsonResponseBody(cache2Lon.pushStateStatus());

      assertEquals("CANCELED", pushStatusCache1.at(SFO).asString());
      assertEquals("CANCELED", pushStatusCache2.at(SFO).asString());
   }

   @Test
   public void testXsiteView() {
      assertXSiteView(jsonResponseBody(clientPerSite.get(LON).cacheManager(CACHE_MANAGER).info()));
      assertXSiteView(jsonResponseBody(clientPerSite.get(NYC).cacheManager(CACHE_MANAGER).info()));
      assertXSiteView(jsonResponseBody(clientPerSite.get(SFO).cacheManager(CACHE_MANAGER).info()));
   }

   private void assertXSiteView(Json rsp) {
      Map<String, Json> json = rsp.asJsonMap();
      assertTrue(json.get("relay_node").asBoolean());
      List<Object> view = json.get("sites_view").asList();
      assertTrue(view.contains(LON));
      assertTrue(view.contains(NYC));
      assertTrue(view.contains(SFO));
      assertEquals(3, view.size());
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

      Json json = jsonResponseBody(cacheClient.backupStatus(backup));
      return json.at(cacheManagerAddress).asString();
   }

   private void assertAllSitesOnline(RestClient restClient, String... sites) {
      Json json = jsonResponseBody(restClient.cacheManager(CACHE_MANAGER).backupStatuses());
      Arrays.stream(sites).forEach(s -> assertEquals(json.at(s).at("status").asString(), "online"));
   }

   @Override
   protected void afterSitesCreated() {
      // LON backs-up to SFO, NYC
      ConfigurationBuilder builder = defaultConfigurationForSite(0);
      builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC)
            .stateTransfer().chunkSize(5);
      builder.sites().addBackup().site(siteName(2)).strategy(BackupConfiguration.BackupStrategy.SYNC)
            .stateTransfer().chunkSize(5);
      defineCaches(0, builder.build());
      defineCaches(2, builder.build());
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
      defineCaches(1, builder.build());
      site(1).waitForClusterToForm(CACHE_1);
      site(1).waitForClusterToForm(CACHE_2);
   }

   private void defineCaches(int siteIndex, Configuration configuration) {
      EmbeddedCacheManagerAdmin admin = manager(siteIndex, 0).administration().withFlags(VOLATILE);
      admin.getOrCreateCache(CACHE_1, configuration);
      admin.getOrCreateCache(CACHE_2, configuration);
   }

   // unable to use org.infinispan.remoting.transport.ControlledTransport since it blocks non-blocking threads and the test hangs
   // the non-blocking threads are shared with Netty and channel read-writes hangs in there.
   public static class BlockXSitePushStateTransport extends AbstractDelegatingTransport {

      @GuardedBy("this")
      private final List<Runnable> pendingCommands;
      @GuardedBy("this")
      private boolean enabled;

      private BlockXSitePushStateTransport(Transport actual) {
         super(actual);
         this.pendingCommands = new ArrayList<>(2);
         this.enabled = false;
      }

      public static BlockXSitePushStateTransport replace(Cache<?, ?> cache) {
         return replace(cache.getCacheManager());
      }

      public static BlockXSitePushStateTransport replace(EmbeddedCacheManager manager) {
         log.tracef("Replacing transport on %s", manager.getAddress());
         Transport t = extractGlobalComponent(manager, Transport.class);
         if (t instanceof BlockXSitePushStateTransport) {
            return (BlockXSitePushStateTransport) t;
         }
         return wrapGlobalComponent(manager, Transport.class, BlockXSitePushStateTransport::new, true);
      }

      @Override
      public void start() {
         //avoid starting again
      }

      @Override
      public void stop() {
         log.trace("Stopping BlockXSitePushStateTransport");
         super.stop();
      }

      public synchronized void startBlocking() {
         log.trace("Start blocking XSiteStatePushCommand");
         this.enabled = true;
      }

      public void stopBlocking() {
         log.trace("Stop blocking XSiteStatePushCommand");
         List<Runnable> copy;
         synchronized (this) {
            this.enabled = false;
            copy = new ArrayList<>(pendingCommands);
            pendingCommands.clear();
         }
         copy.forEach(Runnable::run);
      }

      public synchronized void waitForCommand() throws InterruptedException, TimeoutException {
         log.trace("Waiting for XSiteStatePushCommand");
         long endTime = TIME_SERVICE.expectedEndTime(30, TimeUnit.SECONDS);
         long timeLeftMillis;
         while (pendingCommands.isEmpty() && (timeLeftMillis = TIME_SERVICE.remainingTime(endTime, TimeUnit.MILLISECONDS)) > 0) {
            this.wait(timeLeftMillis);
         }
         if (pendingCommands.isEmpty()) {
            throw new TimeoutException();
         }
      }

      @Override
      public <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteReplicateCommand<O> rpcCommand) {
         synchronized (this) {
            if (enabled && rpcCommand instanceof XSiteStatePushCommand) {
               XSiteResponseImpl<O> toReturn = new XSiteResponseImpl<>(TIME_SERVICE, backup);
               pendingCommands.add(() -> {
                  XSiteResponse<O> real = super.backupRemotely(backup, rpcCommand);
                  real.whenComplete((o, throwable) -> toReturn.accept(SuccessfulResponse.create(o), throwable));
               });
               this.notifyAll();
               return toReturn;
            }
         }
         return super.backupRemotely(backup, rpcCommand);
      }
   }
}
