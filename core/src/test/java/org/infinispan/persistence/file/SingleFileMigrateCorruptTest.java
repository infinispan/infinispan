package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test that replicates 11.x -> 12.1.x corrupt migration.
 * <p>
 * To reproduce `sfs/corrupt/migrate_corrupt_test.dat` execute {@link SFSCreator11_0} on the 11.x branch and then
 * execute {@link SFSCorruptedMigration12_1} with 12.1.4.Final or below.
 *
 * @author Ryan Emerson
 * @since 13.0
 */
@Test(groups = "unit", testName = "persistence.file.SingleFileMigrateCorruptTest")
public class SingleFileMigrateCorruptTest extends AbstractInfinispanTest {

   static final String CACHE_NAME = "update-cache";

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

   public void testAllEntriesRecovered() throws Exception {
      InputStream is = FileLookupFactory.newInstance().lookupFile("sfs/corrupt/migrate_corrupt_test.dat", Thread.currentThread().getContextClassLoader());
      Files.copy(is, Paths.get(tmpDirectory).resolve(CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);

      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
      cbh.newConfigurationBuilder(CACHE_NAME)
            .persistence()
            .addSingleFileStore()
            .segmented(false)
            .location(tmpDirectory);

      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);

         // Ensure all expected values are readable
         assertContent(cache.getAdvancedCache(), false);

         // Write to a migrated key
         cache.put(0, "RuntimeValue");

         // Create a new key
         cache.put("NewKey", "NewValue");
      }

      // Start it up a second time to make sure the migrated data is properly read still
      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
         // Ensure all expected values are readable
         assertContent(cache.getAdvancedCache(), true);

         // Ensure that the entry updated after migration can be read
         assertEquals("RuntimeValue", cache.get(0));

         // Ensure that the entry created after migration can be read
         assertEquals("NewValue", cache.get("NewKey"));
      }
   }

   private void assertContent(AdvancedCache<Object, Object> cache, boolean skip0) {
      IntStream.range(skip0 ? 1 : 0, 1000).forEach(i -> {
         if (i % 4 == 0) {
            // Ensure that we have the expected values from SFSCorruptedMigration12_1
            assertEquals(i + "-updated-12.1", cache.get(i));
         } else {
            // Ensure that we have the expected values from SFSCreator11_0
            assertEquals(i, cache.get(i));
         }
      });

      IntStream.range(1000, 2000).forEach(i -> {
         CacheEntry<Object, Object> entry = cache.getCacheEntry(i);
         if (i % 8 == 0) {
            assertNotNull("No entry found for key: " + i, entry);
            // Ensure that we have the expected values from SFSCorruptedMigration12_1
            assertEquals(i + "-updated-12.1", entry.getValue());

            // We overrode the expiration lifespan
            assertEquals(-1, entry.getLifespan());
         } else if (i % 4 == 0) {
            // These should have expired since they were inserted after the corruption occurred
           assertNull("Entry was supposed to be expired for key: " + i, entry);
         } else {
            assertNotNull("No entry found for key: " + i, entry);
            // Ensure that we have the expected values from SFSCreator11_0
            assertEquals(i, entry.getValue());

            assertEquals(longTimeMilliseconds(), entry.getLifespan());
         }
      });

      IntStream.range(2000, 3000).forEach(i -> {
         if (i % 4 == 0) {
            assertEquals(i + "-12.1-with-expiration", cache.get(i));
         } else if (i % 2 == 0) {
            assertNull(cache.get(i));
         } else {
            assertEquals(i + "-12.1", cache.get(i));
         }
      });
   }

   static long longTimeMilliseconds() {
      return TimeUnit.DAYS.toMillis(365 * 50);
   }
}

/**
 * Used to generate initial .dat file on 11.x branch
 * <p>
 * Migrate and manipulate .dat file created by {@link SFSCreator11_0}. Results in a corrupted .dat file
 */
//final class SFSCreator11_0 {
//   static final String CACHE_NAME = "update-cache";
//   public static void main(String[] args) throws Exception {
//      Path sfsPath = Paths.get("/tmp/sfs");
//      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
//      cbh.newConfigurationBuilder(CACHE_NAME)
//            .persistence()
//            .addSingleFileStore()
//            .purgeOnStartup(true)
//            .segmented(false)
//            .location(sfsPath.toString());
//
//      // Total number of entries 2003
//      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
//         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
//         SingleFileStore<Object, Object> store = TestingUtil.getWriter(cache, 0);
//         MarshallableEntryFactory<Object, Object> mef = TestingUtil.extractComponent(cache, MarshallableEntryFactory.class);
//
//         // Primitive values
//         IntStream.range(0, 1000).forEach(i -> cache.put(i, i));
//
//         // Values with expiration.
//         IntStream.range(1000, 2000).forEach(i -> cache.put(i, i, SingleFileMigrateCorruptTest.longTimeMilliseconds(), TimeUnit.MILLISECONDS));
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

/**
 * Migrate and manipulate .dat file created by {@link SFSCreator11_0}. Results in a corrupted .dat file
 */
//final class SFSCorruptedMigration12_1  {
//   static final String CACHE_NAME = "update-cache";
//   public static void main(String[] args) throws Exception {
//      Path sfsPath = Paths.get("/tmp/sfs");
//      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
//      cbh.newConfigurationBuilder(CACHE_NAME)
//            .persistence()
//            .addSingleFileStore()
//            .segmented(false)
//            .location(sfsPath.toString());
//
//      // Total number of entries 3003
//      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
//         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
//
//         // Primitive values
//         IntStream.range(0, 1000).forEach(i -> {
//            // Overwrite a subset of values
//            if (i % 4 == 0)
//               cache.put(i, i + "-updated-12.1");
//         });
//
//         // Values with expiration. Need to verify that this is still loaded and timestamp restarted.
//         IntStream.range(1000, 2000).forEach(i -> {
//            // Overwrite a subset of values
//            if (i % 8 == 0) {
//               cache.put(i, i + "-updated-12.1");
//            } else if (i % 4 == 0) {
//               cache.put(i, i + "-updated-12.1", 1, TimeUnit.SECONDS);
//            }
//         });
//
//         IntStream.range(2000, 3000).forEach(i -> {
//            if (i % 4 == 0) {
//               cache.put(i, i + "-12.1-with-expiration", SingleFileMigrateCorruptTest.longTimeMilliseconds(), TimeUnit.MILLISECONDS);
//            } else if (i % 2 == 0) {
//               cache.put(i, i + "-12.1-with-expiration", 1, TimeUnit.SECONDS);
//            } else {
//               cache.put(i, i + "-12.1");
//            }
//         });
//         // Don't update PrivateMetadata entries as that will result in runtime exception
//      }
//   }
//}
