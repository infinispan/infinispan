package org.infinispan.persistence.sifs;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.lambda.NamedLambdas;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.persistence.sifs.configuration.DataConfiguration;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStoreRestartTest")
public class SoftIndexFileStoreRestartTest extends BaseDistStoreTest<Integer, String, SoftIndexFileStoreRestartTest> {
   protected String tmpDirectory;
   protected int fileSize;
   protected boolean hasPassivation;

   {
      // We don't really need a cluster
      INIT_CLUSTER_SIZE = 1;
      l1CacheEnabled = false;
      segmented = true;

      cleanup = CleanupPhase.AFTER_METHOD;
   }

   SoftIndexFileStoreRestartTest fileSize(int fileSize) {
      this.fileSize = fileSize;
      return this;
   }

   SoftIndexFileStoreRestartTest hasPassivation(boolean passivation) {
      this.hasPassivation = passivation;
      return this;
   }

   @Override
   public Object[] factory() {
      return Stream.of(CacheMode.DIST_SYNC, CacheMode.LOCAL)
            .flatMap(type -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                  .flatMap(hasPassivation -> Stream.builder()
                        .add(new SoftIndexFileStoreRestartTest().hasPassivation(hasPassivation).fileSize(1_000).cacheMode(type))
                        .add(new SoftIndexFileStoreRestartTest().hasPassivation(hasPassivation).fileSize(10_000).cacheMode(type))
                        .add(new SoftIndexFileStoreRestartTest().hasPassivation(hasPassivation).fileSize(320_000).cacheMode(type))
                        .add(new SoftIndexFileStoreRestartTest().hasPassivation(hasPassivation).fileSize(2_000_000).cacheMode(type))
                        .add(new SoftIndexFileStoreRestartTest().hasPassivation(hasPassivation).fileSize(DataConfiguration.MAX_FILE_SIZE.getDefaultValue()).cacheMode(type))
                        .build()
                  )
            ).toArray();
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fileSize", "hasPassivation");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fileSize, hasPassivation);
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
      if (cleanup == CleanupPhase.AFTER_TEST) {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();
      if (cleanup == CleanupPhase.AFTER_METHOD) {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      // We don't support shared for SIFS
      assert !shared;
      persistenceConfigurationBuilder.passivation(hasPassivation);
      return persistenceConfigurationBuilder.addSoftIndexFileStore()
            // Force some extra files
            .maxFileSize(fileSize)
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString());
   }

   public void testRestartWithNoIndex() throws Throwable {
      int size = 10;
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put(i, "value-" + i);
      }
      assertEquals(size, cache(0, cacheName).size());

      // Add in an extra remove to make things more interesting, originally this caused compcation to always error
      cache(0, cacheName).remove(4);

      killMember(0, cacheName);

      // Delete the index which should force it to rebuild
      Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));

      createCacheManagers();

      assertEquals(size - 1, cache(0, cacheName).size());
      for (int i = 0; i < size; i++) {
         if (i == 4) {
            assertNull(cache(0, cacheName).get(i));
         } else {
            assertEquals("value-" + i, cache(0, cacheName).get(i));
         }
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

      long actualSize = SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, cacheName);

      SoftIndexFileStoreTestUtils.StatsValue stats = SoftIndexFileStoreTestUtils.readStatsFile(tmpDirectory, cacheName, log);

      assertEquals(actualSize, stats.getStatsSize());

      // Make sure the previous size is the same
      if (previousUsedSize >= 0) {
         assertEquals("Restart attempt: " + iterationCount, previousUsedSize, actualSize - stats.getFreeSize());
      }
      runnable.run();
      // Recreate the cache manager for next run(s)
      createCacheManagers();
      return actualSize - stats.getFreeSize();
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

      cache(0, cacheName).remove(3);

      killMember(0, cacheName);
      // NOTE: we keep the index, so we ensure upon restart that it is correct
      long actualSize = SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, cacheName);

      SoftIndexFileStoreTestUtils.StatsValue stats = SoftIndexFileStoreTestUtils.readStatsFile(tmpDirectory, cacheName, log);

      assertEquals(actualSize, stats.getStatsSize());

      createCacheManagers();

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // Force compaction for the previous file
      CompletionStages.join(compactor.forceCompactionForAllNonLogFiles());

      assertEquals("value-" + (size - 1), cache(0, cacheName).get(key));

      killMember(0, cacheName);

      actualSize = SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, cacheName);

      stats = SoftIndexFileStoreTestUtils.readStatsFile(tmpDirectory, cacheName, log);

      assertEquals(actualSize, stats.getStatsSize());

      // Other tests need a cache manager still
      createCacheManagers();
   }

   @Test(dataProvider = "booleans")
   public void testRestartSameKey(boolean deleteIndex) throws Throwable {
      int size = 20;
      for (int i = 0; i < size; i++) {
         cache(0, cacheName).put("same-key", "value-" + i);
      }

      killMember(0, cacheName);

      if (deleteIndex) {
         // Delete the index which should force it to rebuild
         Util.recursiveFileRemove(Paths.get(tmpDirectory, "index"));
      }
      createCacheManagers();
   }

   public void testRestartCompactorNotComplete() throws Throwable {
      if (fileSize > 320_000) {
         // Don't test larger as tests take way too long
         return;
      }
      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache(0, cacheName));

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      Mocks.blockingFieldMock(checkPoint, Index.class, compactor, Compactor.class, "index",
            (stubber, index) -> stubber.when(index).handleRequest(any()));

      cache(0, cacheName).remove("removed-key");
      int size = 0;
      ConcurrentMap<Integer, Compactor.Stats> stats;
      // Insert until compactor has filled a file - which we can force compaction on below
      while ((stats = compactor.getFileStats()).isEmpty()) {
         cache(0, cacheName).put("key-" + size, "value-" + size);
         size++;
      }

      Integer fileId = stats.keySet().iterator().next();

      CompletionStage<Void> compactorStage = compactor.forceCompactionForAllNonLogFiles();

      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      Future<Void> killFuture = fork(() -> killMember(0, cacheName));

      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

      compactorStage.toCompletableFuture().get(10, TimeUnit.SECONDS);

      killFuture.get(10, TimeUnit.SECONDS);

      createCacheManagers();

      Path dataPath = Path.of(tmpDirectory, "data", cacheName, "data");
      File[] dataFiles = dataPath.toFile().listFiles();

      for (File file : dataFiles) {
         if (file.getName().equals(NonBlockingSoftIndexFileStore.PREFIX_LATEST + fileId)) {
            fail("File " + fileId + " that was compacted is still present!");
         }
      }

      assertNull(cache(0, cacheName).get("removed-key"));

      for (int i = 0; i < size; i++) {
         assertEquals("value-" + i, cache(0, cacheName).get("key-" + i));
      }
   }
}
