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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
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
      configurationBuilder.clustering().hash().numSegments(1);
      return configurationBuilder.persistence()
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .build();
   }

   // test for ISPN-5753
   public void testOverrideWithExpirableAndCompaction() throws InterruptedException {
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

      if (compactor.getFiles().size() == 0) {
         fail("Compactor needs to have more than one file, had: " + compactor.getFileStats());
      }

      Index index = TestingUtil.extractField(compactor, "index");

      FlowableProcessor<IndexRequest>[] processors = TestingUtil.extractField(index, "flowableProcessors");
      assertEquals(1, processors.length);

      FlowableProcessor<IndexRequest> original = processors[0];

      Queue<IndexRequest> queue = new ArrayDeque<>();
      processors[0] = UnicastProcessor.create();
      processors[0].serialize().subscribe(queue::add);

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
}
