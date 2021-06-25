package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCIImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * SFS content created on 11.0.x branch using the below code
 * <p>
 * sfs/corrupt/all-entries.dat contains all off below entries sfs/corrupt/no-private-metadata.dat does not include the
 * final two entries with PrivateMetadata sfs/corrupt/private-metadata-only.dat only contains the final two entries with
 * PrivateMetadata
 * <p>
 * These files were loaded using 12.1.x SFS so that they were migrated with the broken code. Hence, when the .dat files
 * are loaded in this test the SFS attempts to recover the corrupt data.
 *
 * @author Ryan Emerson
 * @since 13.0
 */
@Test(groups = "unit", testName = "persistence.file.SingleFileGracefulShutdownMigrationTest")
public class SingleFileGracefulShutdownMigrationTest extends AbstractInfinispanTest {

   private static final String CACHE_NAME = "update-cache";

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() throws IOException {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      new File(tmpDirectory).mkdirs();
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @DataProvider(name = "testFiles")
   Object[][] singleTypes() {
      return new Object[][] {
            {"sfs/corrupt/all-entries.dat"},
            {"sfs/corrupt/private-metadata-only.dat"},
            {"sfs/corrupt/no-private-metadata.dat"},
            {"sfs/11_0/sfs-store-cache.dat"},
            {"sfs/12_0/all-entries.dat"},
            {"sfs/12_1/all-entries.dat"},
      };
   }

   @Test(dataProvider = "testFiles")
   public void testAllEntriesRecovered(String fileName) throws Exception {
      InputStream is = FileLookupFactory.newInstance().lookupFile(fileName, Thread.currentThread().getContextClassLoader());
      Files.copy(is, Paths.get(tmpDirectory).resolve(CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);

      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
      cbh.getGlobalConfigurationBuilder().serialization().addContextInitializer(new TestDataSCIImpl());
      cbh.newConfigurationBuilder(CACHE_NAME)
            .persistence()
            .addSingleFileStore()
            .segmented(false)
            .location(tmpDirectory);

      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
         // Iterate all entries to ensure values can be read
         cache.forEach((k, v) -> {
            assertNotNull(k);
            assertNotNull(v);
         });
      }

      // Start it up a second time to make sure the migrated data is properly read still

      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
         // Iterate all entries to ensure values can be read
         cache.forEach((k, v) -> {
            assertNotNull(k);
            assertNotNull(v);
         });
      }
   }
}
