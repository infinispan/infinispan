package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.SitesConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.BackupForConfigTest")
public class BackupForConfigTest extends SingleCacheManagerTest {

   ConfigurationBuilder nycBackup;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder lonGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      lonGc.site().localSite("LON");
      TestCacheManagerFactory.amendDefaultCache(lonGc);
      ConfigurationBuilder lon = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      lon.sites().addBackup()
            .site("NYC")
            .strategy(BackupConfiguration.BackupStrategy.SYNC);
      nycBackup = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      nycBackup.sites().backupFor().remoteSite("NYC").remoteCache(lonGc.defaultCacheName().get());

      // Remember to not do nothing else other than
      // creating the cache manager in order to avoid leaks
      return TestCacheManagerFactory.createClusteredCacheManager(lonGc, lon);
   }

   public void testBackupForIsCorrect() {
      cacheManager.getCache(); //start default cache
      cacheManager.defineConfiguration("nycBackup", nycBackup.build());
      cacheManager.getCache("nycBackup");
      SitesConfiguration sitesConfig = cache("nycBackup").getCacheConfiguration().sites();
      assertEquals(getDefaultCacheName(), sitesConfig.backupFor().remoteCache());
      assertEquals("NYC", sitesConfig.backupFor().remoteSite());
      sitesConfig.backupFor().isBackupFor("NYC", getDefaultCacheName());
   }
}
