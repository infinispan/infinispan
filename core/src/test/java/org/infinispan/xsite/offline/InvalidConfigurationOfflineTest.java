package org.infinispan.xsite.offline;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.OfflineStatus;
import org.infinispan.xsite.status.DefaultTakeOfflineManager;
import org.infinispan.xsite.status.TakeOfflineManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests if the site is taken offline in case of incorrect cache configuration from the remote site.
 *
 * @author Pedro Ruivo
 * @since 12
 */
@Test(groups = "xsite", testName = "xsite.offline.InvalidConfigurationOfflineTest")
public class InvalidConfigurationOfflineTest extends AbstractMultipleSitesTest {

   @Override
   protected int defaultNumberOfSites() {
      return 2;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return 1;
   }

   @Override
   protected void afterSitesCreated() {
      waitForSites(sites.stream().map(TestSite::getSiteName).toArray(String[]::new));
   }

   @DataProvider(name = "data")
   public Object[][] collectionItemProvider() {
      return new Object[][]{
            {"not-defined-true", true, RemoteSiteMode.NOT_DEFINED},
            {"not-started-true", true, RemoteSiteMode.NOT_STARTED},
            {"not-clustered-true", true, RemoteSiteMode.LOCAL_CACHE},
            {"not-defined-false", false, RemoteSiteMode.NOT_DEFINED},
            {"not-started-false", false, RemoteSiteMode.NOT_STARTED},
            {"not-clustered-false", false, RemoteSiteMode.LOCAL_CACHE},
      };
   }

   @Test(dataProvider = "data")
   public void testTakeOffline(String cacheName, boolean takeOfflineEnabled, RemoteSiteMode remoteSiteMode) {
      configureSite1(cacheName, takeOfflineEnabled);
      configureSite2(cacheName, remoteSiteMode);

      final OfflineStatus offlineStatus = takeOfflineManager(cacheName).getOfflineStatus(siteName(1));
      assertEquals(takeOfflineEnabled, offlineStatus.isEnabled());
      assertFalse(offlineStatus.isOffline());

      cache(0, cacheName, 0).put("key", "value");
      eventually(() -> "Invalid configuration should take site offline.", offlineStatus::isOffline);
   }

   private DefaultTakeOfflineManager takeOfflineManager(String cacheName) {
      return (DefaultTakeOfflineManager) TestingUtil.extractComponent(cache(0, cacheName, 0), TakeOfflineManager.class);
   }

   private void configureSite2(String cacheName, RemoteSiteMode remoteSiteMode) {
      switch (remoteSiteMode) {
         case LOCAL_CACHE:
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.clustering().cacheMode(CacheMode.LOCAL);
            defineInSite(site(1), cacheName, builder.build());
            site(1).waitForClusterToForm(cacheName);
            assertTrue(site(1).cacheManagers().get(0).isRunning(cacheName));
            return;
         case NOT_DEFINED:
            assertFalse(site(1).cacheManagers().get(0).cacheExists(cacheName));
            return;
         case NOT_STARTED:
            //it defines the cache but doesn't start it.
            defineInSite(site(1), cacheName, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build());
            assertFalse(site(1).cacheManagers().get(0).isRunning(cacheName));
            return;
         default:
            fail("Unexpected: " + remoteSiteMode);
      }
   }

   private void configureSite1(String cacheName, boolean takeOfflineEnabled) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      BackupConfigurationBuilder backupBuilder = builder.sites().addBackup();
      backupBuilder.site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.ASYNC);
      if (takeOfflineEnabled) {
         // we want the take-offline enabled but prevent it for messing with the test.
         backupBuilder.takeOffline().afterFailures(Integer.MAX_VALUE);
      }
      defineInSite(site(0), cacheName, builder.build());
      site(0).waitForClusterToForm(cacheName);
   }

   private enum RemoteSiteMode {
      NOT_DEFINED,
      NOT_STARTED,
      LOCAL_CACHE
   }
}
