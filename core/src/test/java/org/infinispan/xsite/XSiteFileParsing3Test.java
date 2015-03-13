package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "xsite.XSiteFileParsing3Test")
public class XSiteFileParsing3Test extends SingleCacheManagerTest {
   public static final String FILE_NAME = "configs/xsite/xsite-offline-test.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml(FILE_NAME);
   }

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      assertEquals(dcc.sites().allBackups().size(), 1);
      testDefault(dcc);
   }

   public void testInheritor() {
      Configuration dcc = cacheManager.getCacheConfiguration("inheritor");
      testDefault(dcc);
   }

   public void testNoTakeOffline() {
      Configuration dcc = cacheManager.getCacheConfiguration("noTakeOffline");
      assertEquals(1, dcc.sites().allBackups().size());
      BackupConfiguration nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true).create();
      assertTrue(dcc.sites().allBackups().contains(nyc));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }
   public void testTakeOfflineDifferentConfig() {
      Configuration dcc = cacheManager.getCacheConfiguration("takeOfflineDifferentConfig");
      assertEquals(1, dcc.sites().allBackups().size());
      BackupConfigurationBuilder nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.IGNORE).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true);
      nyc.takeOffline().afterFailures(321).minTimeToWait(3765);
      assertTrue(dcc.sites().allBackups().contains(nyc.create()));

   }

   private void testDefault(Configuration dcc) {
      BackupConfigurationBuilder nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.IGNORE).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true);
      nyc.takeOffline().afterFailures(123).minTimeToWait(5673);
      assertTrue(dcc.sites().allBackups().contains(nyc.create()));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }
}
