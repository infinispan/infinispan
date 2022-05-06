package org.infinispan.xsite;

import static org.testng.Assert.assertEquals;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
@Test(groups = {"functional", "xsite"}, testName = "xsite.XSiteCacheConfigurationTest")
public class XSiteCacheConfigurationTest {

   public static final String LON = "LON-1";
   public static final String NYC = "NYC-2";
   public static final String SFO = "SFO-3";

   public void testApi() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.
         sites().addBackup()
            .site(LON)
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
         .sites().addBackup()
            .site(SFO)
         .sites().addBackup()
            .site(NYC);
      assertEquals(cb.sites().backups().size(), 3);
      BackupConfigurationBuilder backup0 = cb.sites().backups().get(0);
      assertEquals(backup0.site(), LON);
      assertEquals(backup0.strategy(), BackupConfiguration.BackupStrategy.SYNC);

      BackupConfigurationBuilder backup1 = cb.sites().backups().get(1);
      assertEquals(backup1.site(), SFO);
      assertEquals(backup1.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      BackupConfigurationBuilder backup2 = cb.sites().backups().get(2);
      assertEquals(backup2.site(), NYC);
      assertEquals(backup2.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      Configuration b = cb.build();
      assertEquals(b.sites().allBackups().size(), 3);
      BackupConfiguration b0 = b.sites().allBackups().get(0);
      assertEquals(b0.site(), LON);
      assertEquals(b0.strategy(), BackupConfiguration.BackupStrategy.SYNC);

      BackupConfiguration b1 = b.sites().allBackups().get(1);
      assertEquals(b1.site(), SFO);
      assertEquals(b1.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      BackupConfigurationBuilder b2 = cb.sites().backups().get(2);
      assertEquals(b2.site(), NYC);
      assertEquals(b2.strategy(), BackupConfiguration.BackupStrategy.ASYNC);
   }

   @Test (expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: Multiple sites have the same name 'LON-1'. This configuration is not valid.")
   public void testSameBackupDefinedMultipleTimes() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.
            sites().addBackup()
               .site(LON)
               .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addBackup()
               .site(LON)
            .sites().addBackup()
               .site(NYC);
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: Backup configuration must include a 'site'.")
   public void testBackupSiteNotSpecified() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.
            sites().addBackup()
               .site();
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: You must specify a 'failure-policy-class' to use a custom backup failure policy for backup 'LON-1'.")
   public void testCustomBackupFailurePolicyClassNotSpecified() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.
            sites().addBackup()
               .site(LON)
               .backupFailurePolicy(BackupFailurePolicy.CUSTOM)
               .failurePolicyClass();
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: Two-phase commit can only be used with synchronous backup strategy.")
   public void testTwoPhaseCommitAsyncBackup() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.
            sites().addBackup()
               .site(LON)
               .strategy(BackupConfiguration.BackupStrategy.ASYNC)
               .useTwoPhaseCommit(true);
      cb.build();
   }

   public void testMultipleCachesWithNoCacheName() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.
            sites().addBackup()
               .site(LON)
               .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addBackup()
               .site(SFO)
               .sites().addBackup()
            .site(NYC);
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: Cross-site replication not available for local cache.")
   public void testLocalCache() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.sites().addBackup().site(LON);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: Cross-site replication not available for local cache.")
   public void testLocalCacheWithBackupFor() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.sites().backupFor().remoteCache("remote").remoteSite(LON);
      builder.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class, expectedExceptionsMessageRegExp = "ISPN\\d+: The XSiteEntryMergePolicy is missing. The cache configuration must include a merge policy.")
   public void testNullXSiteEntryMergePolicy() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.sites().mergePolicy(null);
      builder.build();
   }
}
