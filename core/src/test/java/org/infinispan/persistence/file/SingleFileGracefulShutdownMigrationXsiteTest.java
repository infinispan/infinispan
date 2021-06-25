package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.xsite.statetransfer.AbstractStateTransferTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * A test to ensure that the PrivateMetadata instances created as part of the 12_0 -> 12_1 SFS migrateCorruptDataV12_0
 * allow recovered entries to be read/write xsite after startup.
 *
 * @author Ryan Emerson
 * @since 13.0
 */
@Test(groups = "unit", testName = "persistence.file.SingleFileGracefulShutdownMigrationXsiteTest")
public class SingleFileGracefulShutdownMigrationXsiteTest extends AbstractStateTransferTest {

   private String tmpDirectory;
   private String lonDirectory;
   private String nycDirectory;

   public SingleFileGracefulShutdownMigrationXsiteTest() {
      this.initialClusterSize = 1;
      this.nycBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      this.lonBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      this.implicitBackupCache = true;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      new File(tmpDirectory).mkdirs();
      File f = new File(tmpDirectory, LON);
      lonDirectory = f.getPath();
      f.mkdirs();

      f = new File(tmpDirectory, NYC);
      nycDirectory = f.getPath();
      f.mkdirs();

      try (InputStream nyc = FileLookupFactory.newInstance().lookupFile("sfs/corrupt/xsite/nyc.dat", Thread.currentThread().getContextClassLoader());
           InputStream lon = FileLookupFactory.newInstance().lookupFile("sfs/corrupt/xsite/lon.dat", Thread.currentThread().getContextClassLoader())) {
         Files.copy(lon, Paths.get(lonDirectory).resolve(TestCacheManagerFactory.DEFAULT_CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);
         Files.copy(nyc, Paths.get(nycDirectory).resolve(TestCacheManagerFactory.DEFAULT_CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      super.createBeforeClass();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void clearContent() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory);
   }

   private ConfigurationBuilder cfg(String location) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.persistence()
            .addSingleFileStore()
            .segmented(false)
            .location(location)
            .fetchPersistentState(true);
      return builder;
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return cfg(nycDirectory);
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return cfg(lonDirectory);
   }

   @Test
   public void testAllEntriesRecovered() {
      Cache<Object, Object> lonCache = cache(LON, 0);
      Cache<Object, Object> nycCache = cache(NYC, 0);

      startStateTransfer(lonCache, NYC);
      startStateTransfer(nycCache, LON);

      assertEventuallyStateTransferNotRunning();
      assertEventuallyStateTransferNotRunning(cache(NYC, 0));

      assertInSite(LON, cache -> assertEquals("sfs-value-lon", cache.get(LON)));
      assertInSite(LON, cache -> assertEquals("sfs-value-nyc", cache.get(NYC)));
      assertInSite(NYC, cache -> assertEquals("sfs-value-lon", cache.get(LON)));
      assertInSite(NYC, cache -> assertEquals("sfs-value-nyc", cache.get(NYC)));

      // Update entries
      lonCache.put(LON, "Updated");
      assertEventuallyInSite(LON, cache -> Objects.equals("Updated", cache.get(LON)), 30, TimeUnit.SECONDS);
      assertEventuallyInSite(NYC, cache -> Objects.equals("Updated", cache.get(LON)), 30, TimeUnit.SECONDS);

      nycCache.put(NYC, "Updated");
      assertEventuallyInSite(LON, cache -> Objects.equals("Updated", cache.get(NYC)), 30, TimeUnit.SECONDS);
      assertEventuallyInSite(NYC, cache -> Objects.equals("Updated", cache.get(NYC)), 30, TimeUnit.SECONDS);
   }
}
