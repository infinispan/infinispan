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
@Test(groups = "functional", testName = "xsite.XSiteFileParsing2Test")
public class XSiteFileParsing2Test extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-test2.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml(FILE_NAME);
   }

   public void testDefaultCache() {
      Configuration dcc = cacheManager.getDefaultCacheConfiguration();
      assertEquals(dcc.sites().allBackups().size(), 3);
      assertEquals(dcc.sites().enabledBackups().size(), 2);
      testDefault(dcc);
   }

   public void testInheritor() {
      Configuration dcc = cacheManager.getCacheConfiguration("inheritor");
      testDefault(dcc);
   }

   public void testNoBackupFor() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackupFor");
      assertEquals(1, dcc.sites().allBackups().size());
      BackupConfiguration nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true).create();
      assertTrue(dcc.sites().allBackups().contains(nyc));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }

   public void testNoBackupFor2() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackupFor2");
      assertEquals(0, dcc.sites().allBackups().size());
   }

   private void testDefault(Configuration dcc) {
      BackupConfiguration nyc = new BackupConfigurationBuilder(null).site("NYC").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.IGNORE).failurePolicyClass(null).replicationTimeout(12003)
            .useTwoPhaseCommit(false).enabled(true).create();
      BackupConfiguration sfo = new BackupConfigurationBuilder(null).site("SFO").strategy(BackupStrategy.ASYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(10000)
            .useTwoPhaseCommit(false).enabled(true).create();
      BackupConfiguration lon = new BackupConfigurationBuilder(null).site("LON").strategy(BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.WARN).failurePolicyClass(null).replicationTimeout(10000)
            .useTwoPhaseCommit(false).enabled(false).create();
      assertTrue(dcc.sites().allBackups().contains(nyc));
      assertTrue(dcc.sites().allBackups().contains(sfo));
      assertTrue(dcc.sites().allBackups().contains(lon));
      assertTrue(dcc.sites().enabledBackups().contains(nyc));
      assertTrue(dcc.sites().enabledBackups().contains(sfo));
      assertTrue(!dcc.sites().enabledBackups().contains(lon));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }

}
