package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.Cache;
import org.infinispan.commons.lambda.NamedLambdas;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@InCacheMode({CacheMode.DIST_SYNC, CacheMode.LOCAL})
@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreRestartTest")
public class SoftIndexFileStoreRestartTest extends BaseDistStoreTest<Integer, String, SoftIndexFileStoreRestartTest> {
   protected String tmpDirectory;

   {
      // We don't really need a cluster
      INIT_CLUSTER_SIZE = 1;
      l1CacheEnabled = false;
      segmented = true;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
      super.createBeforeClass();
   }


   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      // We don't support shared for SIFS
      assert !shared;
      return persistenceConfigurationBuilder.addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString());
   }

   public void testRestartWithNoIndex() throws Throwable {
      int size = 10;
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put(i, "value-" + i);
      }
      assertEquals(size, cache(0, cacheName).size());

      killMember(0, cacheName);

      // Delete the index which should force it to rebuild
      Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));

      createCacheManagers();

      assertEquals(size, cache(0, cacheName).size());
      for (int i = 0; i < size; i++) {
         assertEquals("value-" + i, cache(0, cacheName).get(i));
      }
   }

   static <K, V> NonBlockingSoftIndexFileStore<K, V> getStoreFromCache(Cache<K, V> cache) {
      WaitDelegatingNonBlockingStore<K, V> storeDel = TestingUtil.getFirstStoreWait(cache);
      return (NonBlockingSoftIndexFileStore<K, V>) storeDel.delegate();
   }

   @DataProvider(name = "restart")
   Object[][] restartProvider() {
      return new Object[][]{
            {NamedLambdas.of("DELETE", () -> {
               Path path = Path.of(tmpDirectory, "index", cacheName, "index", "index.stats");
               path.toFile().delete();
            })},
            {NamedLambdas.of("NO-DELETE", () -> {
            })}
      };
   }

   long dataDirectorySize() {
      Path dataPath = Path.of(tmpDirectory, "data", cacheName, "data");
      File[] dataFiles = dataPath.toFile().listFiles();

      long length = 0;
      for (File file : dataFiles) {
         length += file.length();
      }
      return length;
   }

   @Test(dataProvider = "restart")
   public void testStatsUponRestart(Runnable runnable) throws Throwable {
      int attempts = 5;
      long previousSize = -1;

      for (int i = 0; i < attempts; ++i) {
         previousSize = performRestart(runnable, previousSize, i);
      }
   }

   long performRestart(Runnable runnable, long previousUsedSize, int iterationCount) throws Throwable {
      log.debugf("Iteration: %s", iterationCount);
      int size = 10;
      Cache<Integer, String> cache = cache(0, cacheName);
      for (int i = 0; i < size; i++) {
         // Skip a different insert each time (after first)
         if (iterationCount > 0 && i != iterationCount) {
            continue;
         }
         String prev = cache.put(i, "iteration-" + iterationCount + " value-" + i);
         if (iterationCount > 0) {
            assertNotNull(prev);
            // TODO: https://issues.redhat.com/browse/ISPN-13969 to fix this
//            assertEquals("iteration-" + (iterationCount - 1) + " value-" + i, prev);
         }
      }
      assertEquals(size, cache.size());

      killMember(0, cacheName);

      long actualSize = dataDirectorySize();

      long statsSize = 0;
      long freeSize = 0;
      try (FileChannel statsChannel = new RandomAccessFile(
            Path.of(tmpDirectory, "index", cacheName, "index", "index.stats").toFile(), "r").getChannel()) {
         ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 8);
         while (Index.read(statsChannel, buffer)) {
            buffer.flip();
            // Ignore id
            int file = buffer.getInt();
            int length = buffer.getInt();
            int free = buffer.getInt();
            // Ignore expiration
            buffer.getLong();
            buffer.flip();

            statsSize += length;
            freeSize += free;
            log.debugf("File: %s Length: %s free: %s", file, length, free);
         }
      }

      assertEquals(actualSize, statsSize);

      // Make sure the previous size is the same
      if (previousUsedSize >= 0) {
         assertEquals("Restart attempt: " + iterationCount, previousUsedSize, actualSize - freeSize);
      }
      runnable.run();
      // Recreate the cache manager for next run(s)
      createCacheManagers();
      return actualSize - freeSize;
   }

   @DataProvider(name = "booleans")
   Object[][] booleans() {
      return new Object[][]{
            {Boolean.TRUE}, {Boolean.FALSE}};
   }

   @Test(dataProvider = "booleans")
   public void testRestartWithEntryUpdatedMultipleTimes(boolean leafOrNode) throws Throwable {
      int size = 10;
      String key = "compaction";
      // We want to test both a leaf and node storage on the root node
      int extraInserts = leafOrNode ? size : size * 256;
      for (int i = 0; i < extraInserts; i++) {
         // Have some extra entries which prevent it from running compaction at beginning
         cache(0, cacheName).put(i, "value-" + i);
      }
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put(key, "value-" + i);
      }
      assertEquals(extraInserts + 1, cache(0, cacheName).size());

      killMember(0, cacheName);
      // NOTE: we keep the index, so we ensure upon restart that it is correct

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals("value-" + (size - 1), cache(0, cacheName).get(key));
   }
}
