package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertNotNull;

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
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
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

   //   private static final String DATA_FILE = "sfs/corrupt/all-entries.dat";
   private static final String DATA_FILE = "sfs/corrupt/no-private-metadata.dat";
   //   private static final String DATA_FILE = "sfs/corrupt/private-metadata-only.dat";
   private static final String CACHE_NAME = "update-cache";

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() throws IOException {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      Files.createDirectory(Paths.get(tmpDirectory));
   }

   @AfterClass
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @BeforeMethod
   public void setup() throws IOException {
      InputStream is = FileLookupFactory.newInstance().lookupFile(DATA_FILE, Thread.currentThread().getContextClassLoader());
      Files.copy(is, Paths.get(tmpDirectory).resolve(CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);
   }

   public void testAllEntriesRecovered() throws Exception {
      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
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
   }
}

//public class SingleFileStorePopulator {
//   public static void main(String[] args) throws Exception {
//      Path sfsPath = Paths.get("/home/remerson/workspace/RedHat/infinispan/12.1.x/core/src/test/resources/sfs/corrupt/");
//      String cacheName = "update-cache";
//      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
//      cbh.newConfigurationBuilder(cacheName)
//            .persistence()
//            .addSingleFileStore()
//            .purgeOnStartup(true)
//            .segmented(false)
//            .location(sfsPath.toString());
//
//      // Generate 11.x .dat file
//      // Then use this to ensure that migration works with the various scenarios
//      // Save 12.1.x .dat file after broken migration and use this for tests
//      // Fix broken migration code
//      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
//         Cache<Object, Object> cache = cacheManager.getCache(cacheName);
//         SingleFileStore<Object, Object> store = TestingUtil.getWriter(cache, 0);
//         MarshallableEntryFactory<Object, Object> mef = TestingUtil.extractComponent(cache, MarshallableEntryFactory.class);
//
//         // Primitive values
//         IntStream.range(0, 1000).forEach(i -> cache.put(i, i));
//
//         // Values with expiration. Need to verify that this is still loaded and timestamp restarted.
//         IntStream.range(1000, 2000).forEach(i -> cache.put(i, i, 1, TimeUnit.MINUTES));
//
//         // WrappedByteArrays
//         WrappedByteArray wba = new WrappedByteArray("wrapped-bytes".getBytes(StandardCharsets.UTF_8));
//         cache.put(wba, wba);
//
//         // Async Xsite PrivateMetadata entry
//         DefaultIracVersionGenerator iracVersionGenerator = new DefaultIracVersionGenerator();
//         TestingUtil.replaceField("site-name", "localSite", iracVersionGenerator, DefaultIracVersionGenerator.class);
//         PrivateMetadata privateMetadata = new PrivateMetadata.Builder()
//               .iracMetadata(iracVersionGenerator.generateNewMetadata(2))
//               .build();
//         MarshallableEntry<Object, Object> me = mef.create("irac-key", "irac-value", null, privateMetadata, -1, -1);
//         store.write(me);
//
//         // Optimistic Tx PrivateMetadata entry
//         privateMetadata = new PrivateMetadata.Builder()
//               .entryVersion(new NumericVersionGenerator().generateNew())
//               .build();
//         me = mef.create("opt-tx-key", "opt-tx-value", null, privateMetadata, -1, -1);
//         store.write(me);
//      }
//   }
//}
