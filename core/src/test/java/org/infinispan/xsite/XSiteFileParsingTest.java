package org.infinispan.xsite;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.BackupForConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.configuration.cache.XSiteStateTransferConfigurationBuilder.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "xsite.XSiteFileParsingTest")
public class XSiteFileParsingTest extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-test.xml";
   private static final TakeOfflineConfiguration DEFAULT_TAKE_OFFLINE = new TakeOfflineConfiguration(0, 0);
   private static final XSiteStateTransferConfiguration DEFAULT_STATE_TRANSFER =
         new XSiteStateTransferConfiguration(DEFAULT_CHUNK_SIZE, DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES, DEFAULT_WAIT_TIME);

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

      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC2", BackupConfiguration.BackupStrategy.SYNC,
                                                                           160000, BackupFailurePolicy.CUSTOM,
                                                                           CountingCustomFailurePolicy.class.getName(),
                                                                           false, DEFAULT_TAKE_OFFLINE ,
                                                                           DEFAULT_STATE_TRANSFER, true)));
      assertEquals(dcc.sites().backupFor().remoteCache(), null);
   }

   private void testDefault(Configuration dcc) {
      assertEquals(dcc.sites().allBackups().size(), 2);
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null, false,
                                                                        DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true)));
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("SFO", BackupConfiguration.BackupStrategy.ASYNC,
                                                                        10000l, BackupFailurePolicy.WARN, null, false,
                                                                        DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true)));
   }
}
