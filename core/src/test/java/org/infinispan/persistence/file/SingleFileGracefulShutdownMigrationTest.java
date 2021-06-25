package org.infinispan.persistence.file;

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * SFS content created on multiple branches using the below code:
 * <pre>
 *    public static void main(String[] args) throws Exception {
 *       // TODO update to required destination folder
 *       Path sfsPath = Paths.get();
 *       String cacheName = "update-cache";
 *       ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
 * //      TODO Uncomment for `all-entries-java-serialization.dat`
 * //      cbh.getGlobalConfigurationBuilder().serialization().marshaller(new JavaSerializationMarshaller());
 *       cbh.newConfigurationBuilder(cacheName)
 *             .persistence()
 *             .addSingleFileStore()
 *             .purgeOnStartup(true)
 *             .segmented(false)
 *             .location(sfsPath.toString());
 *
 *       // Generate 11.x .dat file
 *       // Then use this to ensure that migration works with the various scenarios
 *       // Save 12.1.x .dat file after broken migration and use this for tests
 *       // Fix broken migration code
 *       try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
 *          Cache<Object, Object> cache = cacheManager.getCache(cacheName);
 *          SingleFileStore<Object, Object> store = TestingUtil.getWriter(cache, 0);
 *          MarshallableEntryFactory<Object, Object> mef = TestingUtil.extractComponent(cache, MarshallableEntryFactory.class);
 *
 *          // Primitive values
 *          IntStream.range(0, 1000).forEach(i -> cache.put(i, i));
 *
 *          // Values with expiration. Need to verify that this is still loaded and timestamp restarted.
 *          IntStream.range(1000, 2000).forEach(i -> cache.put(i, i, 1, TimeUnit.MINUTES));
 *
 *          // WrappedByteArrays
 *          WrappedByteArray wba = new WrappedByteArray("wrapped-bytes".getBytes(StandardCharsets.UTF_8));
 *          cache.put(wba, wba);
 *
 *          // Async Xsite PrivateMetadata entry
 *          DefaultIracVersionGenerator iracVersionGenerator = new DefaultIracVersionGenerator("");
 *          TestingUtil.replaceField("site-name", "localSite", iracVersionGenerator, DefaultIracVersionGenerator.class);
 *          PrivateMetadata privateMetadata = new PrivateMetadata.Builder()
 *                .iracMetadata(iracVersionGenerator.generateNewMetadata(2))
 *                .build();
 *          MarshallableEntry<Object, Object> me = mef.create("irac-key", "irac-value", null, privateMetadata, -1, -1);
 *          store.write(me);
 *
 *          // Optimistic Tx PrivateMetadata entry
 *          privateMetadata = new PrivateMetadata.Builder()
 *                .entryVersion(new NumericVersionGenerator().generateNew())
 *                .build();
 *          me = mef.create("opt-tx-key", "opt-tx-value", null, privateMetadata, -1, -1);
 *          store.write(me);
 *       }
 *    }
 * </pre>
 * <p>
 * Files stored in `sfs/corrupt` contain already corrupted .dat files that were originally created in 11.x and were
 * migrated to 12.1.x with the broken SFS migration code. Hence, when these .dat files are loaded the SFS attempts to
 * recover the corrupt data.
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

   enum Marshaller {
      PROTOSTREAM,
      JAVA_SERIALIZATION
   }

   @DataProvider(name = "testFiles")
   Object[][] singleTypes() {
      return new Object[][]{
            // Not possible to test corrupt Marshaller.JAVA_SERIALIZATION bytes as previously 11.x -> 12. migration always fails with non-protostream marshallers
            // MAGIC_11_0
            {"sfs/11_0/all-entries-java-serialization.dat", Marshaller.JAVA_SERIALIZATION},
            // MAGIC_12_0 created on 12.1.x
            {"sfs/12_0/all-entries-java-serialization.dat", Marshaller.JAVA_SERIALIZATION},
            // 11.x -> 12.1.x migration resulting in corrupt data
            {"sfs/corrupt/all-entries.dat", Marshaller.PROTOSTREAM},
            // MAGIC_12_0 created on 12.1.x
            {"sfs/12_0/all-entries.dat", Marshaller.PROTOSTREAM},
      };
   }

   @Test(dataProvider = "testFiles")
   public void testAllEntriesRecovered(String fileName, Marshaller marshallerType) throws Exception {
      int numEntries = 1003;
      InputStream is = FileLookupFactory.newInstance().lookupFile(fileName, Thread.currentThread().getContextClassLoader());
      Files.copy(is, Paths.get(tmpDirectory).resolve(CACHE_NAME + ".dat"), StandardCopyOption.REPLACE_EXISTING);

      ConfigurationBuilderHolder cbh = new ConfigurationBuilderHolder();
      if (marshallerType == Marshaller.JAVA_SERIALIZATION)
         cbh.getGlobalConfigurationBuilder().serialization().marshaller(new JavaSerializationMarshaller());

      cbh.newConfigurationBuilder(CACHE_NAME)
            .persistence()
            .addSingleFileStore()
            .segmented(false)
            .location(tmpDirectory);

      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
         ByRef.Integer i = new ByRef.Integer(0);
         // Iterate all entries to ensure values can be read
         cache.forEach((k, v) -> i.inc());
         assertEquals(numEntries, i.get());

         // Write to a migrated key
         cache.put(0, "RuntimeValue");

         // Create a new key
         cache.put("NewKey", "NewValue");
      }

      // Start it up a second time to make sure the migrated data is properly read still
      try (EmbeddedCacheManager cacheManager = new DefaultCacheManager(cbh, true)) {
         Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
         ByRef.Integer i = new ByRef.Integer(0);
         // Iterate all entries to ensure values can be read
         cache.forEach((k, v) -> i.inc());
         assertEquals(numEntries + 1, i.get());

         // Ensure that the entry updated after migration can be read
         assertEquals("RuntimeValue", cache.get(0));

         // Ensure that the entry created after migration can be read
         assertEquals("NewValue", cache.get("NewKey"));
      }
   }
}
