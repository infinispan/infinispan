package org.infinispan.xsite;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public abstract class BaseSiteUnreachableTest extends AbstractXSiteTest {

   protected BackupFailurePolicy lonBackupFailurePolicy = BackupFailurePolicy.WARN;
   protected BackupConfiguration.BackupStrategy lonBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
   protected String lonCustomFailurePolicyClass = null;
   protected int failures = 0;


   @Override
   protected void createSites() {

      GlobalConfigurationBuilder lonGc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      lonGc
            .site().localSite("LON");
      ConfigurationBuilder lon = getLonActiveConfig();
      lon.sites().addBackup()
               .site("NYC")
               .backupFailurePolicy(lonBackupFailurePolicy)
               .replicationTimeout(100) //keep it small so that the test doesn't take long to run
               .takeOffline().afterFailures(failures).
            backup()
               .strategy(lonBackupStrategy)
               .failurePolicyClass(lonCustomFailurePolicyClass);
      lon.sites().addInUseBackupSite("NYC");

      createSite("LON", 2, lonGc, lon);
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }
}
