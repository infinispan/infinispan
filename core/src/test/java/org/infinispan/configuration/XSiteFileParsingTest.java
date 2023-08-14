package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.BackupForConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.xsite.CountingCustomFailurePolicy;
import org.infinispan.xsite.CustomXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.AlwaysRemoveXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.DefaultXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.PreferNonNullXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.PreferNullXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "xsite.XSiteFileParsingTest")
public class XSiteFileParsingTest extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-test.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml(FILE_NAME, false, true, TransportFlags.minimalXsiteFlags());
   }

   public void testLocalSiteName() {
      cacheManager.getTransport().checkCrossSiteAvailable();
      assertEquals("LON-1", cacheManager.getTransport().localSiteName());
   }

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      testDefault(dcc);
   }

   public void testBackupNyc() {
      Configuration dcc = cacheManager.getCacheConfiguration("backupNyc");
      assertEquals(0, dcc.sites().allBackups().size());
      BackupForConfiguration backupForConfiguration = dcc.sites().backupFor();
      assertEquals("someCache", backupForConfiguration.remoteCache());
      assertEquals("NYC", backupForConfiguration.remoteSite());
   }

   public void testInheritor() {
      Configuration dcc = cacheManager.getCacheConfiguration("inheritor");
      testDefault(dcc);
   }

   public void testNoBackups() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackups");
      assertEquals(0, dcc.sites().allBackups().size());
      assertNull(dcc.sites().backupFor().remoteCache());
      assertNull(dcc.sites().backupFor().remoteSite());
   }

   public void testCustomBackupPolicy() {
      Configuration dcc = cacheManager.getCacheConfiguration("customBackupPolicy");
      assertEquals(1, dcc.sites().allBackups().size());
      BackupConfigurationBuilder nyc2 = new BackupConfigurationBuilder(null).site("NYC2").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.CUSTOM)
            .failurePolicyClass(CountingCustomFailurePolicy.class.getName()).replicationTimeout(160000)
            .useTwoPhaseCommit(false);

      assertTrue(dcc.sites().allBackups().contains(nyc2.create()));
      assertNull(dcc.sites().backupFor().remoteCache());
   }

   public void testXSiteMergePolicy() {
      Configuration dcc = cacheManager.getCacheConfiguration("conflictResolver");
      assertEquals(1, dcc.sites().allBackups().size());
      assertEquals(PreferNonNullXSiteEntryMergePolicy.getInstance(), dcc.sites().mergePolicy());
   }

   public void testXSiteMergePolicy2() {
      Configuration dcc = cacheManager.getCacheConfiguration("conflictResolver2");
      assertEquals(1, dcc.sites().allBackups().size());
      assertEquals(PreferNullXSiteEntryMergePolicy.getInstance(), dcc.sites().mergePolicy());
   }

   public void testXSiteMergePolicy3() {
      Configuration dcc = cacheManager.getCacheConfiguration("conflictResolver3");
      assertEquals(1, dcc.sites().allBackups().size());
      assertEquals(AlwaysRemoveXSiteEntryMergePolicy.getInstance(), dcc.sites().mergePolicy());
   }

   public void testCustomXSiteMergePolicy() {
      Configuration dcc = cacheManager.getCacheConfiguration("customConflictResolver");
      assertEquals(1, dcc.sites().allBackups().size());
      assertEquals(CustomXSiteEntryMergePolicy.class, dcc.sites().mergePolicy().getClass());
      Cache<?, ?> cache = cacheManager.getCache("customConflictResolver");
      XSiteEntryMergePolicy<? ,?> resolver = cache.getAdvancedCache().getComponentRegistry().getComponent(XSiteEntryMergePolicy.class);
      assertEquals(CustomXSiteEntryMergePolicy.class, resolver.getClass());
   }

   public void testAutoStateTransfer() {
      Configuration dcc = cacheManager.getCacheConfiguration("autoStateTransfer");
      assertEquals(1, dcc.sites().allBackups().size());
      assertEquals(XSiteStateTransferMode.AUTO, dcc.sites().allBackups().get(0).stateTransfer().mode());
   }

   public void testTombstoneConfiguration() {
      Configuration dcc = cacheManager.getCacheConfiguration("tombstoneCleanup");
      assertEquals(3000, dcc.sites().maxTombstoneCleanupDelay());
      assertEquals(4000, dcc.sites().tombstoneMapSize());
   }


   private void testDefault(Configuration dcc) {
      assertEquals(2, dcc.sites().allBackups().size());
      assertEquals(DefaultXSiteEntryMergePolicy.getInstance(), dcc.sites().mergePolicy());
      BackupConfigurationBuilder nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.IGNORE).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false);
      assertTrue(dcc.sites().allBackups().contains(nyc.create()));
      BackupConfigurationBuilder sfo = new BackupConfigurationBuilder(null).site("SFO").strategy(BackupStrategy.ASYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(15000)
            .useTwoPhaseCommit(false);
      assertTrue(dcc.sites().allBackups().contains(sfo.create()));
   }
}
