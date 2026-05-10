package org.infinispan.xsite;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
      assertEquals(3, cb.sites().backups().size());
      BackupConfigurationBuilder backup0 = cb.sites().backups().get(0);
      assertEquals(LON, backup0.site());
      assertEquals(BackupConfiguration.BackupStrategy.SYNC, backup0.strategy());

      BackupConfigurationBuilder backup1 = cb.sites().backups().get(1);
      assertEquals(SFO, backup1.site());
      assertEquals(BackupConfiguration.BackupStrategy.ASYNC, backup1.strategy());

      BackupConfigurationBuilder backup2 = cb.sites().backups().get(2);
      assertEquals(NYC, backup2.site());
      assertEquals(BackupConfiguration.BackupStrategy.ASYNC, backup2.strategy());

      Configuration b = cb.build();
      assertEquals(3, b.sites().allBackups().size());
      BackupConfiguration b0 = b.sites().allBackups().get(0);
      assertEquals(LON, b0.site());
      assertEquals(BackupConfiguration.BackupStrategy.SYNC, b0.strategy());

      BackupConfiguration b1 = b.sites().allBackups().get(1);
      assertEquals(SFO, b1.site());
      assertEquals(BackupConfiguration.BackupStrategy.ASYNC, b1.strategy());

      BackupConfigurationBuilder b2 = cb.sites().backups().get(2);
      assertEquals(NYC, b2.site());
      assertEquals(BackupConfiguration.BackupStrategy.ASYNC, b2.strategy());
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
