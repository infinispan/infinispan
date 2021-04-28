package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.transaction.LockingMode;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class AbstractTwoSitesTest extends AbstractXSiteTest {

   protected static final String LON = "LON-1";
   protected static final String NYC = "NYC-2";
   protected BackupFailurePolicy lonBackupFailurePolicy = BackupFailurePolicy.WARN;
   protected boolean isLonBackupTransactional = false;
   protected BackupConfiguration.BackupStrategy lonBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
   protected BackupConfiguration.BackupStrategy nycBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
   protected String lonCustomFailurePolicyClass = null;
   protected boolean use2Pc = false;
   protected int initialClusterSize = 2;

   /**
    * If true, the caches from one site will backup to a cache having the same name remotely (mirror) and the backupFor
    * config element won't be used.
    */
   protected boolean implicitBackupCache = false;
   protected CacheMode cacheMode;
   protected boolean transactional;
   protected LockingMode lockingMode;

   @Override
   protected void createSites() {
      ConfigurationBuilder lon = lonConfigurationBuilder();

      ConfigurationBuilder nyc = getNycActiveConfig();
      nyc.sites().addBackup()
            .site(LON)
            .strategy(nycBackupStrategy)
            .sites().addInUseBackupSite(LON);

      createSite(LON, initialClusterSize, globalConfigurationBuilderForSite(LON), lon);
      createSite(NYC, initialClusterSize, globalConfigurationBuilderForSite(NYC), nyc);

      if (!implicitBackupCache) {
         ConfigurationBuilder nycBackup = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
         nycBackup.sites().backupFor().remoteSite(NYC).remoteCache(getDefaultCacheName());
         startCache(LON, "nycBackup", nycBackup);
         ConfigurationBuilder lonBackup = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, isLonBackupTransactional);
         lonBackup.sites().backupFor().remoteSite(LON).remoteCache(getDefaultCacheName());
         startCache(NYC, "lonBackup", lonBackup);
         Configuration lonBackupConfig = cache(NYC, "lonBackup", 0).getCacheConfiguration();
         assertTrue(lonBackupConfig.sites().backupFor().isBackupFor(LON, getDefaultCacheName()));
      }
      waitForSites(LON, NYC);
   }

   protected ConfigurationBuilder lonConfigurationBuilder() {
      ConfigurationBuilder lon = getLonActiveConfig();
      BackupConfigurationBuilder lonBackupConfigurationBuilder = lon.sites().addBackup();
      lonBackupConfigurationBuilder
            .site(NYC)
            .backupFailurePolicy(lonBackupFailurePolicy)
            .strategy(lonBackupStrategy)
            .failurePolicyClass(lonCustomFailurePolicyClass)
            .useTwoPhaseCommit(use2Pc)
            .stateTransfer().maxRetries(5).waitTime(1000)
            .sites().addInUseBackupSite(NYC);
      adaptLONConfiguration(lonBackupConfigurationBuilder);
      return lon;
   }

   protected GlobalConfigurationBuilder globalConfigurationBuilderForSite(String siteName) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      return builder;
   }

   protected void adaptLONConfiguration(BackupConfigurationBuilder builder) {
      //no-op
   }

   protected Cache<Object, Object> backup(String site) {
      return backup(site, 0);
   }

   protected Cache<Object, Object> backup(String site, int offset) {
      if (site.equals(LON)) return implicitBackupCache ? cache(NYC, offset) : cache(NYC, "lonBackup", offset);
      if (site.equals(NYC)) return implicitBackupCache ? cache(LON, offset) : cache(LON, "nycBackup", offset);
      throw new IllegalArgumentException("No such site: " + site);
   }

   protected String val(String site) {
      return "v_" + site;
   }

   protected String key(String site) {
      return "k_" + site;
   }


   public AbstractTwoSitesTest cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }

   public AbstractTwoSitesTest transactional(boolean transactional) {
      this.transactional = transactional;
      return this;
   }

   public AbstractTwoSitesTest lockingMode(LockingMode lockingMode) {
      this.lockingMode = lockingMode;
      return this;
   }

   public AbstractTwoSitesTest use2Pc(boolean use2Pc) {
      this.use2Pc = use2Pc;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return new String[] {null, "tx", "lockingMode", null};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[] {cacheMode, transactional, lockingMode, use2Pc ? "2PC" : null};
   }

   protected abstract ConfigurationBuilder getNycActiveConfig();

   protected abstract ConfigurationBuilder getLonActiveConfig();
}
