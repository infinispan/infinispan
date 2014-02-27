package org.infinispan.xsite;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.TakeOfflineConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager embeddedCacheManager = TestCacheManagerFactory.fromXml(FILE_NAME);
      return embeddedCacheManager;
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
                                                                        12003, BackupFailurePolicy.WARN, null, false, new TakeOfflineConfiguration(0,0), true)));
      assertNull(dcc.sites().backupFor().remoteSite());
      assertNull(dcc.sites().backupFor().remoteCache());
   }

   public void testNoBackupFor2() {
      Configuration dcc = cacheManager.getCacheConfiguration("noBackupFor2");
      assertEquals(0, dcc.sites().allBackups().size());
   }

   private void testDefault(Configuration dcc) {
      BackupConfiguration nyc = new BackupConfiguration("NYC", BackupConfiguration.BackupStrategy.SYNC,
                                                        12003l, BackupFailurePolicy.IGNORE, null, false, new TakeOfflineConfiguration(0,0), true);
      BackupConfiguration sfo = new BackupConfiguration("SFO", BackupConfiguration.BackupStrategy.ASYNC,
                                                        10000l, BackupFailurePolicy.WARN, null, false, new TakeOfflineConfiguration(0,0), true);
      BackupConfiguration lon = new BackupConfiguration("LON", BackupConfiguration.BackupStrategy.SYNC,
                                                        10000l, BackupFailurePolicy.WARN, null, false, new TakeOfflineConfiguration(0,0), false);
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
