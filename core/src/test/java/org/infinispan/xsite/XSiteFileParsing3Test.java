package org.infinispan.xsite;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "xsite.XSiteFileParsing3Test")
public class XSiteFileParsing3Test extends SingleCacheManagerTest {
   public static final String FILE_NAME = "configs/xsite/xsite-offline-test.xml";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml(FILE_NAME);
      return embeddedCacheManager;
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
                                                                        12003, BackupFailurePolicy.WARN, null, false, new TakeOfflineConfiguration(0,0), true)));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }
   public void testTakeOfflineDifferentConfig() {
      Configuration dcc = cacheManager.getCacheConfiguration("takeOfflineDifferentConfig");
      assertEquals(1, dcc.sites().allBackups().size());
      TakeOfflineConfiguration toc = new TakeOfflineConfiguration(321, 3765);
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null, false, toc, true)));

   }

   private void testDefault(Configuration dcc) {
      TakeOfflineConfiguration toc = new TakeOfflineConfiguration(123, 5673);
      assertTrue(dcc.sites().allBackups().contains(new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                                        12003l, BackupFailurePolicy.IGNORE, null, false, toc, true)));
      assertEquals("someCache", dcc.sites().backupFor().remoteCache());
      assertEquals("SFO", dcc.sites().backupFor().remoteSite());
   }
}
