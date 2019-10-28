package org.infinispan.xsite.offline;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.xsite.AbstractXSiteTest;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.BackupSenderImpl;
import org.infinispan.xsite.OfflineStatus;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the async cross-site replication is working properly.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@Test(groups = "functional", testName = "xsite.offline.AsyncTimeBasedOfflineTest")
public class AsyncTimeBasedOfflineTest extends AbstractXSiteTest {

   private static final int NUM_NODES = 3;
   private static final long MIN_WAIT_TIME_MILLIS = 1000;

   private static final String LON = "LON-1";
   private static final String NYC = "NYC-2";
   private static final String SFO = "SFO-3";

   public void testSFOOffline(Method method) {
      String cacheName = method.getName();
      defineCache(LON, cacheName, getLONConfiguration());
      defineCache(NYC, cacheName, getNYCOrSFOConfiguration());

      String key = method.getName() + "-key";
      int primaryOwner = primaryOwnerIndex(cacheName, key);
      for (int i = 0; i < NUM_NODES; ++i) {
         //probably and overkill, but this will test on primary owner, backup owner, sitemaster and non-sitemaster
         doTestInNode(cacheName, i, primaryOwner, key);
      }
   }

   @AfterMethod(alwaysRun = true)
   public void killSFO() {
      killSite(SFO);
   }

   @Override
   protected void createSites() {
      //we have 3 sites: LON, NYC and SFO. SFO is offline.
      createTestSite(LON);
      createTestSite(NYC);
      waitForSites(LON, NYC);
   }

   private void doTestInNode(String cacheName, int index, int primaryOwnerIndex, String key) {
      Cache<String, String> cache = this.cache(LON, cacheName, index);

      assertOnline(cacheName, index, NYC);
      assertOnline(cacheName, index, SFO);

      if (index != primaryOwnerIndex) {
         assertOnline(cacheName, primaryOwnerIndex, NYC);
         assertOnline(cacheName, primaryOwnerIndex, SFO);
      }

      cache.put(key, "value");

      if (index == primaryOwnerIndex) {
         assertOnline(cacheName, index, NYC);
         assertEventuallyOffline(cacheName, index);
      } else {
         assertOnline(cacheName, index, NYC);
         assertOnline(cacheName, index, SFO);

         assertOnline(cacheName, primaryOwnerIndex, NYC);
         assertEventuallyOffline(cacheName, primaryOwnerIndex);
      }

      assertBringSiteOnline(cacheName, primaryOwnerIndex);
   }

   private void assertOnline(String cacheName, int index, String targetSiteName) {
      OfflineStatus status = backupSender(cacheName, index).getOfflineStatus(targetSiteName);
      assertTrue(status.isEnabled());
      assertFalse("Site " + targetSiteName + " is offline. status=" + status, status.isOffline());
   }

   private void assertEventuallyOffline(String cacheName, int index) {
      OfflineStatus status = backupSender(cacheName, index).getOfflineStatus(SFO);
      assertTrue(status.isEnabled());
      eventually(status::minTimeHasElapsed);
      cache(LON, cacheName, index).put("_key_", "_value_");
      assertTrue("Site " + SFO + " is online. status=" + status, status.isOffline());
   }

   private void assertBringSiteOnline(String cacheName, int index) {
      OfflineStatus status = backupSender(cacheName, index).getOfflineStatus(SFO);
      assertTrue("Unable to bring " + SFO + " online. status=" + status, status.bringOnline());
   }

   private BackupSenderImpl backupSender(String cacheName, int index) {
      return (BackupSenderImpl) cache(LON, cacheName, index).getAdvancedCache()
            .getComponentRegistry()
            .getComponent(BackupSender.class);
   }

   private int primaryOwnerIndex(String cacheName, String key) {
      for (int i = 0; i < NUM_NODES; ++i) {
         boolean isPrimary = cache(LON, cacheName, i).getAdvancedCache().getComponentRegistry()
               .getDistributionManager()
               .getCacheTopology()
               .getDistribution(key)
               .isPrimary();
         if (isPrimary) {
            return i;
         }
      }
      throw new IllegalStateException();
   }

   private Configuration getLONConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().hash().numSegments(4);
      builder.sites().addBackup()
            .site(NYC)
            .backupFailurePolicy(BackupFailurePolicy.FAIL)
            .replicationTimeout(1000) //keep it small so that the test doesn't take long to run
            .takeOffline()
            .afterFailures(-1)
            .minTimeToWait(MIN_WAIT_TIME_MILLIS)
            .backup()
            .strategy(BackupConfiguration.BackupStrategy.SYNC);
      builder.sites().addInUseBackupSite(NYC);

      builder.sites().addBackup()
            .site(SFO)
            .backupFailurePolicy(BackupFailurePolicy.FAIL)
            .replicationTimeout(1000) //keep it small so that the test doesn't take long to run
            .takeOffline()
            .afterFailures(-1)
            .minTimeToWait(MIN_WAIT_TIME_MILLIS)
            .backup()
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      builder.sites().addInUseBackupSite(SFO);

      return builder.build();
   }

   private Configuration getNYCOrSFOConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build();
   }

   private void defineCache(String siteName, String cacheName, Configuration configuration) {
      TestSite site = site(siteName);
      site.cacheManagers().get(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, configuration);
      site.waitForClusterToForm(cacheName);
   }

   private void createTestSite(String siteName) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.site().localSite(siteName);
      createSite(siteName, NUM_NODES, gcb, new ConfigurationBuilder());
   }

}
