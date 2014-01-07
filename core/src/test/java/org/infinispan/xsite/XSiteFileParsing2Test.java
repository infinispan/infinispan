package org.infinispan.xsite;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.configuration.cache.XSiteStateTransferConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.configuration.cache.XSiteStateTransferConfigurationBuilder.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "xsite.XSiteFileParsing2Test")
public class XSiteFileParsing2Test extends SingleCacheManagerTest {

   public static final String FILE_NAME = "configs/xsite/xsite-test2.xml";
   private static final TakeOfflineConfiguration DEFAULT_TAKE_OFFLINE = new TakeOfflineConfiguration(0, 0);
   private static final XSiteStateTransferConfiguration DEFAULT_STATE_TRANSFER =
         new XSiteStateTransferConfiguration(DEFAULT_CHUNK_SIZE, DEFAULT_TIMEOUT);

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

      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003, BackupFailurePolicy.WARN, null, false,
                                                                        DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true)));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }

   public void testNoBackupFor2() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackupFor2");
      assertEquals(0, dcc.sites().allBackups().size());
   }

   private void testDefault(Configuration dcc) {
      BackupConfiguration nyc = new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                        12003l, BackupFailurePolicy.IGNORE, null, false,
                                                        DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true);
      BackupConfiguration sfo = new BackupConfiguration("SFO", BackupConfiguration.BackupStrategy.ASYNC,
                                                        10000l, BackupFailurePolicy.WARN, null, false,
                                                        DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, true);
      BackupConfiguration lon = new BackupConfiguration("LON", BackupConfiguration.BackupStrategy.SYNC,
                                                        10000l, BackupFailurePolicy.WARN, null, false,
                                                        DEFAULT_TAKE_OFFLINE, DEFAULT_STATE_TRANSFER, false);
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
