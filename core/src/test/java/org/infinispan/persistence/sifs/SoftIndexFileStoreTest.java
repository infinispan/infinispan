package org.infinispan.persistence.sifs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.BaseNonBlockingStoreTest;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.internal.subscriptions.AsyncSubscription;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.reactivex.rxjava3.subscribers.TestSubscriber;

@Test(groups = "unit", testName = "persistence.sifs.SoftIndexFileStoreTest")
public class SoftIndexFileStoreTest extends BaseNonBlockingStoreTest {

   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected NonBlockingStore createStore() {
      return new NonBlockingSoftIndexFileStore();
   }

   @Override
   protected Configuration buildConfig(ConfigurationBuilder configurationBuilder) {
      configurationBuilder.clustering().hash().numSegments(2);
      return configurationBuilder.persistence()
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .build();
   }

   // test for ISPN-5753
   public void testOverrideWithExpirableAndCompaction() {
      // write immortal entry
      store.write(marshalledEntry(internalCacheEntry("key", "value1", -1)));
      writeGibberish(-1, true); // make sure that compaction happens - value1 is compacted
      store.write(marshalledEntry(internalCacheEntry("key", "value2", 1)));
      timeService.advance(2);
      writeGibberish(-1, true); // make sure that compaction happens - value2 expires
      store.stop();
      startStore(store);
      // value1 has been overwritten and value2 has expired
      MarshallableEntry entry = store.loadEntry("key");
      assertNull(entry != null ? entry.getKey() + "=" + entry.getValue() : null, entry);
   }

   private void writeGibberish(long lifespan, boolean shouldDelete) {
      for (int i = 0; i < 100; ++i) {
         store.write(marshalledEntry(internalCacheEntry("foo" + i, "bar", lifespan)));
         if (shouldDelete) {
            store.delete("foo" + i);
         }
      }
   }

   public void testStopWithCompactorIndexNotComplete() throws InterruptedException, ExecutionException, TimeoutException {
      long lifespan = 10;
      store.write(marshalledEntry(internalCacheEntry("never", "dies", -1)));
      writeGibberish(lifespan, false);

      store.write(marshalledEntry(internalCacheEntry("foo" + 0, "bar", -1)));

      timeService.advance(lifespan + 1);

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      if (compactor.getFiles().isEmpty()) {
         fail("Compactor needs to have more than one file, had: " + compactor.getFileStats());
      }

      Index index = TestingUtil.extractField(compactor, "index");

      FlowableProcessor<IndexRequest>[] processors = TestingUtil.extractField(index, "flowableProcessors");
      assertEquals(2, processors.length);

      FlowableProcessor<IndexRequest> original = processors[0];

      Queue<IndexRequest> queue = new ArrayDeque<>();
      UnicastProcessor<IndexRequest> unicastProcessor = UnicastProcessor.create();
      unicastProcessor.serialize().subscribe(queue::add);

      processors[0] = unicastProcessor;

      CountDownLatch latch = new CountDownLatch(1);

      // We don't care about the actual expiration sub info
      Compactor.CompactionExpirationSubscriber expSub = new Compactor.CompactionExpirationSubscriber() {
         @Override
         public void onEntryPosition(EntryPosition entryPosition) { }
         @Override
         public void onEntryEntryRecord(EntryRecord entryRecord) { }
         @Override
         public void onComplete() {
            latch.countDown();
         }
         @Override
         public void onError(Throwable t) {
         }
      };

      fork(() -> compactor.performExpirationCompaction(expSub));

      // Compaction shouldn't complete as indexing wasn't allowed to proceed
      assertFalse(latch.await(100, TimeUnit.MILLISECONDS));

      // Finally let the indexing continue
      processors[0] = original;
      queue.forEach(original::onNext);

      // Now expiration compaction should finish as indexing will occur
      assertTrue(latch.await(10, TimeUnit.SECONDS));

      // The stop method blocks for NBSIS as we are faking the blocking manager
      Future<CompletionStage<Void>> stage = fork(() -> store.stop());

      CompletionStage<Void> innerStage = stage.get(10, TimeUnit.SECONDS);

      innerStage.toCompletableFuture().get(10, TimeUnit.SECONDS);

      // Restart to prevent other test failures
      startStore(store);
   }

   public void testCompactLogFileNotInTemporaryTable() throws InterruptedException, TimeoutException, ExecutionException {
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      LogAppender logAppender = TestingUtil.extractField(store.delegate(), "logAppender");

      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      TemporaryTable original = Mocks.blockingFieldMock(checkPoint, TemporaryTable.class, logAppender, LogAppender.class, "temporaryTable",
            (stubber, temporaryTable) -> stubber.when(temporaryTable).set(anyInt(), any(), anyInt(), anyInt()));

      // Use fork as the index update in the table is blocked
      Future<Void> future = fork(() -> store.write(marshalledEntry(internalCacheEntry("foo", "bar", -1))));

      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      Exceptions.expectException(TimeoutException.class, () -> future.get(10, TimeUnit.MILLISECONDS));

      // Put the original table back so our compaction request can work
      TestingUtil.replaceField(original, "temporaryTable", logAppender, LogAppender.class);

      TestSubscriber<Object> testSubscriber = TestSubscriber.create();
      testSubscriber.onSubscribe(new AsyncSubscription());

      // We don't care about the actual expiration sub info
      Compactor.CompactionExpirationSubscriber expSub = new Compactor.CompactionExpirationSubscriber() {
         @Override
         public void onEntryPosition(EntryPosition entryPosition) { }
         @Override
         public void onEntryEntryRecord(EntryRecord entryRecord) { }
         @Override
         public void onComplete() {
            testSubscriber.onComplete();
         }
         @Override
         public void onError(Throwable t) {
            testSubscriber.onError(t);
         }
      };

      compactor.performExpirationCompaction(expSub);

      testSubscriber.awaitDone(10, TimeUnit.SECONDS)
            .assertComplete().assertNoErrors();

      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

      future.get(10, TimeUnit.SECONDS);
   }

   public void testWriteDuringCompaction() throws Exception {
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      CheckPoint checkPoint = new CheckPoint();

      TemporaryTable ignore = Mocks.blockingFieldMock(checkPoint, TemporaryTable.class, compactor, Compactor.class, "temporaryTable",
            (stubber, table) -> stubber.when(table).get(anyInt(), any()));

      store.write(marshalledEntry(internalCacheEntry("foo", "bar", 10)));

      AtomicInteger expired = new AtomicInteger(0);
      TestSubscriber<Object> testSubscriber = TestSubscriber.create();
      testSubscriber.onSubscribe(new AsyncSubscription());
      // We don't care about the actual expiration sub info
      Compactor.CompactionExpirationSubscriber sub = new Compactor.CompactionExpirationSubscriber() {
         @Override
         public void onEntryPosition(EntryPosition entryPosition) { }
         @Override
         public void onEntryEntryRecord(EntryRecord entryRecord) {
            expired.incrementAndGet();
         }
         @Override
         public void onComplete() {
            testSubscriber.onComplete();
         }
         @Override
         public void onError(Throwable t) {
            testSubscriber.onError(t);
         }
      };

      timeService.advance(11);

      Future<Void> compaction = fork(() -> compactor.performExpirationCompaction(sub));
      checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Compaction is still running.
      testSubscriber.assertNotComplete();

      // We write a new entry, concurrently modifying the file during compaction.
      store.write(marshalledEntry(internalCacheEntry("newer", "entry", 10)));

      // Allow compaction to run.
      checkPoint.trigger(Mocks.BEFORE_RELEASE);

      checkPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS);
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      // Compaction finishes successfully.
      eventually(compaction::isDone);
      compaction.get(10, TimeUnit.SECONDS);
      testSubscriber.awaitDone(10, TimeUnit.SECONDS)
            .assertComplete().assertNoErrors();

      // Only a single entry was expired.
      assertThat(expired.get()).isEqualTo(1);
   }

   public void testRemoveSegmentsCleansUpProperly() throws ExecutionException, InterruptedException, TimeoutException {
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      var fileStats = compactor.getFileStats();
      assertEquals(0, fileStats.size());
      // We force the entry into segment 0
      TestingUtil.join(store.write(0, marshalledEntry(internalCacheEntry("foo", "bar", 10))));

      // Now remove the segment
      TestingUtil.join(store.removeSegments(IntSets.immutableSet(0)));

      assertNull(TestingUtil.join(store.load(0, "foo")));

      verifyStatsHaveNoData(-77, fileStats);

      // Add the segment back... the value should be gone still
      TestingUtil.join(store.addSegments(IntSets.immutableSet(0)));

      assertNull(TestingUtil.join(store.load(0, "foo")));

      // Note this is -77 since we don't set the size of the file until we complete it or shutdown
      verifyStatsHaveNoData(-77, fileStats);

      // Stop the store so we can restart to test if the entry is still gone or not
      store.stopAndWait();

      // Restart to prevent other test failures
      startStore(store);

      // Technically this will fail if the index is deleted... however that is not an issue as this test is
      // really for DIST Cache mode, and the store should ALWAYS have purgeOnStartup enabled which would prevent this from
      // being an issue
      assertNull(TestingUtil.join(store.load(0, "foo")));

      compactor = TestingUtil.extractField(store.delegate(), "compactor");

      // When the store restarted it compacted the file away
      fileStats = compactor.getFileStats();
      assertTrue("fileStats were: " + fileStats, fileStats.isEmpty());

      assertEquals(0, SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, "mock-cache"));
   }

   private void verifyStatsHaveNoData(long expected, ConcurrentMap<Integer, Compactor.Stats> fileStats) {
      long sizeAfterAddingBack = 0;
      // Note stats file still may be empty here
      for (Compactor.Stats stats : fileStats.values()) {
         sizeAfterAddingBack -= stats.getFree();
         if (stats.getTotal() > 0) {
            sizeAfterAddingBack += stats.getTotal();
         }
      }

      assertEquals(expected, sizeAfterAddingBack);
   }

   public void testFileStatsWriteNotOwnedSegment() throws ExecutionException, InterruptedException, TimeoutException {
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      var fileStats = compactor.getFileStats();
      assertEquals(0, fileStats.size());

      TestingUtil.join(store.write(0, marshalledEntry(internalCacheEntry("foo-0", "bar-0", 10))));

      assertTrue(fileStats.isEmpty());

      TestingUtil.join(store.removeSegments(IntSets.immutableSet(1)));

      TestingUtil.join(store.write(1, marshalledEntry(internalCacheEntry("foo-1", "bar-1", 10))));

      // This should contain free data since we wrote an entry to a segment that we no longer own
      verifyStatsHaveNoData(-81, fileStats);
   }

   public void testFileStatsAfterRemovingSegment() throws ExecutionException, InterruptedException, TimeoutException {
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      var fileStats = compactor.getFileStats();
      assertEquals(0, fileStats.size());

      TestingUtil.join(store.write(1, marshalledEntry(internalCacheEntry("foo-1", "bar-1", 10))));

      TestingUtil.join(store.removeSegments(IntSets.immutableSet(1)));

      // This should contain free data since we wrote an entry to a segment that we no longer own
      verifyStatsHaveNoData(-81, fileStats);
   }

   public void testFileStatsAfterRemovingWithRemovedEntry() throws ExecutionException, InterruptedException, TimeoutException {
      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");
      var fileStats = compactor.getFileStats();
      assertEquals(0, fileStats.size());

      TestingUtil.join(store.write(1, marshalledEntry(internalCacheEntry("foo-1", "bar-1", 10))));

      TestingUtil.join(store.delete(1, "foo-1"));

      // Removed entry information doesn't currently count towards free space see ISPN-15246
      verifyStatsHaveNoData(-81, fileStats);

      TestingUtil.join(store.removeSegments(IntSets.immutableSet(1)));

      // After removing the segment even the removed entries are updated in stats
      verifyStatsHaveNoData(-123, fileStats);
   }
}
