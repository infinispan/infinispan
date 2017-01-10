package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.BackupForNotSpecifiedTest")
public class BackupForNotSpecifiedTest extends AbstractXSiteTest {

   @Override
   protected void createSites() {

      GlobalConfigurationBuilder lonGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      lonGc.site().localSite("LON");
      ConfigurationBuilder lonDefault = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      lonDefault.sites().addBackup()
            .site("NYC")
            .backupFailurePolicy(BackupFailurePolicy.FAIL)
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addInUseBackupSite("NYC");
      ConfigurationBuilder someCache = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);

      GlobalConfigurationBuilder nycGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      nycGc.site().localSite("NYC");
      ConfigurationBuilder nycDefault = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      nycDefault.sites().addBackup()
            .site("LON")
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addInUseBackupSite("LON");
      ConfigurationBuilder someCacheBackup = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      someCacheBackup.sites().backupFor().remoteCache("someCache").remoteSite("LON");
      someCacheBackup.sites().disableBackups(true);

      createSite("LON", 2, lonGc, lonDefault);
      createSite("NYC", 2, nycGc, nycDefault);

      startCache("LON", "backup", getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true));
      startCache("NYC", "backup", getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true));

      startCache("LON", "someCache", someCache);
      startCache("NYC", "someCacheBackup", someCacheBackup);
   }

   public void testDataGetsReplicated() {
      cache("LON", 0).put("k_default_lon", "v_default_lon");
      assertEquals("v_default_lon", cache("LON", 1).get("k_default_lon"));
      assertEquals("v_default_lon", cache("NYC", 0).get("k_default_lon"));
      assertEquals("v_default_lon", cache("NYC", 1).get("k_default_lon"));

      cache("NYC", 0).put("k_default_nyc", "v_default_nyc");
      assertEquals("v_default_nyc", cache("NYC", 1).get("k_default_nyc"));
      assertEquals("v_default_nyc", cache("LON", 0).get("k_default_nyc"));
      assertEquals("v_default_nyc", cache("LON", 1).get("k_default_nyc"));

      cache("LON", "backup", 0).put("k_backup_lon", "v_backup_lon");
      assertEquals("v_backup_lon", cache("LON", "backup", 1).get("k_backup_lon"));
      assertEquals("v_backup_lon", cache("NYC", "backup", 0).get("k_backup_lon"));
      assertEquals("v_backup_lon", cache("NYC", "backup", 1).get("k_backup_lon"));

      cache("NYC", "backup", 0).put("k_backup_nyc", "v_backup_nyc");
      assertEquals("v_backup_nyc", cache("NYC", "backup", 1).get("k_backup_nyc"));
      assertEquals("v_backup_nyc", cache("LON", "backup", 0).get("k_backup_nyc"));
      assertEquals("v_backup_nyc", cache("LON", "backup", 1).get("k_backup_nyc"));

      cache("LON", "someCache", 0).put("k_someCache_lon", "v_someCache_lon");
      assertEquals("v_someCache_lon", cache("LON", "someCache", 1).get("k_someCache_lon"));
      assertEquals("v_someCache_lon", cache("NYC", "someCacheBackup", 0).get("k_someCache_lon"));
      assertEquals("v_someCache_lon", cache("NYC", "someCacheBackup", 1).get("k_someCache_lon"));

      cache("NYC", "someCacheBackup", 0).put("k_lon_sb", "v_lon_sb");
      assertEquals("v_lon_sb", cache("NYC", "someCacheBackup", 1).get("k_lon_sb"));
   }
}
