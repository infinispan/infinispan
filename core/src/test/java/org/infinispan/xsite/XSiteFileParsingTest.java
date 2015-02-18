package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.BackupConfiguration.BackupStrategy;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.BackupForConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
      return TestCacheManagerFactory.fromXml(FILE_NAME);
   }

   public void testGlobalConfiguration() {
      GlobalConfiguration cmc = cacheManager.getCacheManagerConfiguration();
      assertEquals("LON", cmc.sites().localSite());
   }

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      testDefault(dcc);
   }

   public void testBackupNyc() {
      Configuration dcc = cacheManager.getCacheConfiguration("backupNyc");
      assertEquals(dcc.sites().allBackups().size(), 0);
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
      assertEquals(dcc.sites().allBackups().size(), 0);
      assertEquals(dcc.sites().backupFor().remoteCache(), null);
      assertEquals(dcc.sites().backupFor().remoteSite(), null);
   }

   public void testCustomBackupPolicy() {
      Configuration dcc = cacheManager.getCacheConfiguration("customBackupPolicy");
      assertEquals(dcc.sites().allBackups().size(), 1);
      BackupConfigurationBuilder nyc2 = new BackupConfigurationBuilder(null).site("NYC2").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.CUSTOM).failurePolicyClass(CountingCustomFailurePolicy.class.getName()).replicationTimeout(160000)
            .useTwoPhaseCommit(false).enabled(true);

      assertTrue(dcc.sites().allBackups().contains(nyc2.create()));
      assertEquals(dcc.sites().backupFor().remoteCache(), null);
   }

   private void testDefault(Configuration dcc) {
      assertEquals(dcc.sites().allBackups().size(), 2);
      BackupConfigurationBuilder nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.IGNORE).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true);
      assertTrue(dcc.sites().allBackups().contains(nyc.create()));
      BackupConfigurationBuilder sfo = new BackupConfigurationBuilder(null).site("SFO").strategy(BackupStrategy.ASYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(10000)
            .useTwoPhaseCommit(false).enabled(true);
      assertTrue(dcc.sites().allBackups().contains(sfo.create()));
   }
}
