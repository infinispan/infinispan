package org.infinispan.xsite;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.2
 */
@Test(testName = "xsite.XSiteCacheConfigurationTest", groups = "functional, xsite")
public class XSiteCacheConfigurationTest {

   public void testApi() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
         sites().addBackup()
            .site("LON")
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
         .sites().addBackup()
            .site("SFO")
         .sites().addBackup()
            .site("NYC");
      assertEquals(cb.sites().backups().size(), 3);
      BackupConfigurationBuilder backup0 = cb.sites().backups().get(0);
      assertEquals(backup0.site(), "LON");
      assertEquals(backup0.strategy(), BackupConfiguration.BackupStrategy.SYNC);

      BackupConfigurationBuilder backup1 = cb.sites().backups().get(1);
      assertEquals(backup1.site(), "SFO");
      assertEquals(backup1.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      BackupConfigurationBuilder backup2 = cb.sites().backups().get(2);
      assertEquals(backup2.site(), "NYC");
      assertEquals(backup2.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      Configuration b = cb.build();
      assertEquals(b.sites().allBackups().size(), 3);
      BackupConfiguration b0 = b.sites().allBackups().get(0);
      assertEquals(b0.site(), "LON");
      assertEquals(b0.strategy(), BackupConfiguration.BackupStrategy.SYNC);

      BackupConfiguration b1 = b.sites().allBackups().get(1);
      assertEquals(b1.site(), "SFO");
      assertEquals(b1.strategy(), BackupConfiguration.BackupStrategy.ASYNC);

      BackupConfigurationBuilder b2 = cb.sites().backups().get(2);
      assertEquals(b2.site(), "NYC");
      assertEquals(b2.strategy(), BackupConfiguration.BackupStrategy.ASYNC);
   }

   @Test (expectedExceptions = CacheConfigurationException.class)
   public void testSameBackupDefinedMultipleTimes() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site("LON")
               .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addBackup()
               .site("LON")
            .sites().addBackup()
               .site("NYC");
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testBackupSiteNotSpecified() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site();
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testCustomBackupFailurePolicyClassNotSpecified() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site("LON")
               .backupFailurePolicy(BackupFailurePolicy.CUSTOM)
               .failurePolicyClass();
      cb.build();
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testTwoPhaseCommitAsyncBackup() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site("LON")
               .strategy(BackupConfiguration.BackupStrategy.ASYNC)
               .useTwoPhaseCommit(true);
      cb.build();
   }

   public void testMultipleCachesWithNoCacheName() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.
            sites().addBackup()
               .site("LON")
               .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .sites().addBackup()
               .site("SFO")
               .sites().addBackup()
            .site("NYC");
      cb.build();
   }
}
