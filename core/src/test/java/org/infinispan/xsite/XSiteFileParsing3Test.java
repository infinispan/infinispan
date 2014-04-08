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
import static org.testng.AssertJUnit.*;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "xsite.XSiteFileParsing3Test")
public class XSiteFileParsing3Test extends SingleCacheManagerTest {
   public static final String FILE_NAME = "configs/xsite/xsite-offline-test.xml";
   private static final XSiteStateTransferConfiguration DEFAULT_STATE_TRANSFER =
         new XSiteStateTransferConfiguration(DEFAULT_CHUNK_SIZE, DEFAULT_TIMEOUT, DEFAULT_MAX_RETRIES, DEFAULT_WAIT_TIME);

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

      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003, BackupFailurePolicy.WARN, null, false,
                                                                        new TakeOfflineConfiguration(0, 0),
                                                                        DEFAULT_STATE_TRANSFER, true)));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }
   public void testTakeOfflineDifferentConfig() {
      Configuration dcc = cacheManager.getCacheConfiguration("takeOfflineDifferentConfig");
      assertEquals(1, dcc.sites().allBackups().size());
      TakeOfflineConfiguration toc = new TakeOfflineConfiguration(321, 3765);
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null, false, toc,
                                                                        DEFAULT_STATE_TRANSFER, true)));

   }

   private void testDefault(Configuration dcc) {
      TakeOfflineConfiguration toc = new TakeOfflineConfiguration(123, 5673);
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null, false, toc,
                                                                        DEFAULT_STATE_TRANSFER, true)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }
}
