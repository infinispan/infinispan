package org.infinispan.xsite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.testing.skip.StringLogAppender;
import org.testng.annotations.DataProvider;
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

   public void testSyncBackupReplicationTimeoutGreaterThanRemoteTimeoutThrows() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(10000);
      cb.locking().lockAcquisitionTimeout(5000);
      cb.sites().addBackup().site(LON).strategy(BackupConfiguration.BackupStrategy.SYNC).replicationTimeout(15000);
      assertThatThrownBy(cb::build)
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000734")
            .hasMessageContaining("remote timeout (10000)")
            .hasMessageContaining("backup site '" + LON + "' replication timeout (15000)");
   }

   public void testSyncBackupReplicationTimeoutEqualsRemoteTimeoutWarns() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000);
         cb.locking().lockAcquisitionTimeout(5000);
         cb.sites().addBackup().site(LON).strategy(BackupConfiguration.BackupStrategy.SYNC).replicationTimeout(15000);
         cb.build();
         assertThat(logAppender).anyMatch(line -> line.contains("ISPN000735")
               && line.contains("backup site '" + LON + "' replication timeout (15000)"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testAsyncBackupAllowsHigherReplicationTimeout() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(5000);
      cb.locking().lockAcquisitionTimeout(2000);
      cb.sites().addBackup().site(LON).strategy(BackupConfiguration.BackupStrategy.ASYNC).replicationTimeout(15000);
      Configuration config = cb.build();
      assertThat(config).isNotNull();
   }

   @DataProvider
   public Object[][] nonpositiveBackupTimeouts() {
      return new Object[][] {{"0"}, {"-1"}, {"0.1ms"}, {Long.toString(Long.MIN_VALUE)}};
   }

   @Test(dataProvider = "nonpositiveBackupTimeouts")
   public void testSyncBackupRejectsNonpositiveTimeout(String timeout) {
      for (CacheMode mode : new CacheMode[] {CacheMode.DIST_SYNC, CacheMode.REPL_SYNC}) {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.clustering().cacheMode(mode);
         BackupConfigurationBuilder backup = cb.sites().addBackup().site(LON)
               .strategy(BackupConfiguration.BackupStrategy.SYNC).replicationTimeout(timeout);
         assertThatThrownBy(cb::build)
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000951");
         assertThatThrownBy(() -> backup.validate(GlobalConfigurationBuilder.defaultClusteredBuilder().build()))
               .isInstanceOf(CacheConfigurationException.class)
               .hasMessageContaining("ISPN000951");
      }
   }

   @Test(dataProvider = "nonpositiveBackupTimeouts")
   public void testAsyncBackupAllowsNonpositiveTimeout(String timeout) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.sites().addBackup().site(LON).strategy(BackupConfiguration.BackupStrategy.ASYNC).replicationTimeout(timeout);
      assertThat(cb.build().sites().allBackups().get(0).isAsyncBackup()).isTrue();
   }

   public void testSyncBackupAllowsPositiveTimeout() {
      for (long timeout : new long[] {1, 5000, (long) Integer.MAX_VALUE + 1}) {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(timeout + 15000).stateTransfer().timeout(-1);
         cb.sites().addBackup().site(LON).strategy(BackupConfiguration.BackupStrategy.SYNC).replicationTimeout(timeout);
         assertThat(cb.build().sites().allBackups().get(0).replicationTimeout()).isEqualTo(timeout);
      }
   }

   @Test(dataProvider = "nonpositiveBackupTimeouts")
   public void testSyncBackupRejectsInvalidConfigurationUpdate(String timeout) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      BackupConfigurationBuilder backup = cb.sites().addBackup().site(LON)
            .strategy(BackupConfiguration.BackupStrategy.SYNC).replicationTimeout(5000);
      BackupConfiguration original = cb.build().sites().allBackups().get(0);
      backup.replicationTimeout(timeout);
      BackupConfiguration invalid = cb.build(false).sites().allBackups().get(0);

      assertThatThrownBy(() -> original.validateUpdate("", invalid)).isInstanceOf(CacheConfigurationException.class);
      assertThatThrownBy(() -> original.update("", invalid)).isInstanceOf(CacheConfigurationException.class);
      assertThat(original.replicationTimeout()).isEqualTo(5000);
   }

   @DataProvider(name = "backupLockTimeouts")
   public Object[][] backupLockTimeouts() {
      return new Object[][] {
            {BackupConfiguration.BackupStrategy.SYNC, 15000L, 10000L, 5000L, false},
            {BackupConfiguration.BackupStrategy.SYNC, 15000L, 10000L, 10000L, true},
            {BackupConfiguration.BackupStrategy.SYNC, 15000L, 10000L, 12000L, true},
            {BackupConfiguration.BackupStrategy.SYNC, 15000L, 0L, 12000L, false},
            {BackupConfiguration.BackupStrategy.SYNC, 15000L, -1L, 12000L, false},
            {BackupConfiguration.BackupStrategy.ASYNC, 15000L, 10000L, 12000L, false}
      };
   }

   @Test(dataProvider = "backupLockTimeouts")
   public void testBackupLockTimeoutOrdering(BackupConfiguration.BackupStrategy strategy, long remoteTimeout,
                                             long lockTimeout, long backupTimeout, boolean warning) {
      Thread testThread = Thread.currentThread();
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG", Level.WARN,
            t -> t == testThread, PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder cb = new ConfigurationBuilder();
         cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(remoteTimeout).stateTransfer().timeout(-1);
         cb.locking().lockAcquisitionTimeout(lockTimeout);
         cb.sites().addBackup().site(LON).strategy(strategy).replicationTimeout(backupTimeout);
         cb.build();
         assertThat(logAppender).filteredOn(line -> line.contains("ISPN000745")).hasSize(warning ? 1 : 0);
      } finally {
         logAppender.uninstall();
      }
   }
}
