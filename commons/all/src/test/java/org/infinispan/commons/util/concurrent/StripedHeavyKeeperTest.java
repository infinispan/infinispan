package org.infinispan.commons.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToLongBiFunction;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.stat.HeavyKeeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class StripedHeavyKeeperTest {

   private static final ToLongBiFunction<String, Integer> HASH = (key, seed) -> {
      long h = seed ^ 0x9E3779B97F4A7C15L;
      for (int i = 0; i < key.length(); i++) {
         h = h * 31 + key.charAt(i);
      }
      return h;
   };

   private ExecutorService executor;

   @AfterEach
   public void tearDown() throws Exception {
      if (executor != null) {
         executor.shutdownNow();
         executor.awaitTermination(5, TimeUnit.SECONDS);
      }
   }

   @Test
   public void testMultipleKeysTracked() {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 8, 7, 0.9, HASH);

      for (int i = 0; i < 500; i++) shk.tryAdd("first");
      for (int i = 0; i < 300; i++) shk.tryAdd("second");
      for (int i = 0; i < 100; i++) shk.tryAdd("third");

      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).hasSizeGreaterThanOrEqualTo(3);
      assertThat(top.get(0).key()).isEqualTo("first");
      assertThat(top.get(0).count()).isEqualTo(500);
      assertThat(top.get(1).key()).isEqualTo("second");
      assertThat(top.get(2).key()).isEqualTo("third");
   }

   @Test
   public void testNonPositiveCapacityRejected() {
      // A keeper that tracks no keys would accept every access and silently report nothing.
      assertThatThrownBy(() -> new StripedHeavyKeeper<>(0, 8, 7, 0.9, HASH))
            .as("zero capacity")
            .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> new StripedHeavyKeeper<>(-1, 8, 7, 0.9, HASH))
            .as("negative capacity")
            .isInstanceOf(IllegalArgumentException.class);
   }

   @Test
   public void testTopKeysLimitedToK() {
      int k = 5;
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(k, 8, 7, 0.9, HASH);

      List<String> recorded = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
         String key = "key-" + i;
         recorded.add(key);
         shk.tryAdd(key);
      }

      // Single thread, so every key lands in the same shard with far more keys than k, the top-k is full.
      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).hasSize(k);
      assertThat(top).extracting(HeavyKeeper.KeyFrequency::key).isSubsetOf(recorded);
   }

   @Test
   public void testResetClearsAllShards() {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 8, 7, 0.9, HASH);

      for (int i = 0; i < 200; i++) shk.tryAdd("a");
      for (int i = 0; i < 100; i++) shk.tryAdd("b");

      assertThat(shk.topKeys()).isNotEmpty();

      shk.reset();

      assertThat(shk.topKeys()).isEmpty();
   }

   @Test
   public void testConcurrentTryAddTracksHotKey() throws Exception {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 8, 7, 0.9, HASH);
      int threadCount = 8;
      int opsPerThread = 10_000;
      executor = Executors.newFixedThreadPool(threadCount);

      CountDownLatch startLatch = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(threadCount);

      for (int t = 0; t < threadCount; t++) {
         futures.add(executor.submit(() -> {
            startLatch.await();
            for (int i = 0; i < opsPerThread; i++) {
               shk.tryAdd("hot");
               shk.tryAdd("noise-" + (i % 500));
            }
            return null;
         }));
      }

      startLatch.countDown();
      for (Future<?> f : futures) {
         f.get(30, TimeUnit.SECONDS);
      }

      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).isNotEmpty();
      assertThat(top.get(0).key()).isEqualTo("hot");
   }

   @Test
   public void testConcurrentTryAddMultipleHotKeys() throws Exception {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 16, 7, 0.9, HASH);
      int threadCount = 8;
      int opsPerThread = 10_000;
      executor = Executors.newFixedThreadPool(threadCount);

      CountDownLatch startLatch = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(threadCount);

      for (int t = 0; t < threadCount; t++) {
         futures.add(executor.submit(() -> {
            startLatch.await();
            for (int i = 0; i < opsPerThread; i++) {
               shk.tryAdd("hot-1");
               if (i % 2 == 0) shk.tryAdd("hot-2");
               if (i % 4 == 0) shk.tryAdd("hot-3");
               shk.tryAdd("noise-" + (i % 1000));
            }
            return null;
         }));
      }

      startLatch.countDown();
      for (Future<?> f : futures) {
         f.get(30, TimeUnit.SECONDS);
      }

      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      List<String> topKeys = top.stream().map(HeavyKeeper.KeyFrequency::key).toList();

      assertThat(topKeys).contains("hot-1", "hot-2", "hot-3");

      int idx1 = topKeys.indexOf("hot-1");
      int idx2 = topKeys.indexOf("hot-2");
      int idx3 = topKeys.indexOf("hot-3");
      assertThat(idx1).isLessThan(idx2);
      assertThat(idx2).isLessThan(idx3);
   }

   @Test
   public void testConcurrentWriteAndRead() throws Exception {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 8, 7, 0.9, HASH);
      int writerCount = 8;
      int warmUpOps = 10_000;
      int snapshots = 200;
      executor = Executors.newFixedThreadPool(writerCount + 1);

      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch writersWarmedUp = new CountDownLatch(writerCount);
      AtomicBoolean readerDone = new AtomicBoolean();
      List<Future<?>> futures = new ArrayList<>(writerCount + 1);

      for (int t = 0; t < writerCount; t++) {
         futures.add(executor.submit(() -> {
            startLatch.await();
            // Keep writing until the reader finishes, so every snapshot overlaps the writes.
            int i = 0;
            while (!readerDone.get()) {
               shk.tryAdd("hot");
               shk.tryAdd("noise-" + (i % 500));
               // Count down once, without waiting, after recording "hot" thousands of times.
               if (++i == warmUpOps) {
                  writersWarmedUp.countDown();
               }

               // Yielding to try to give some room when running with fewer cores, not needed for correctness.
               Thread.yield();
            }
            return null;
         }));
      }

      futures.add(executor.submit(() -> {
         try {
            assertThat(writersWarmedUp.await(10, TimeUnit.SECONDS)).isTrue();

            for (int s = 0; s < snapshots; s++) {
               List<HeavyKeeper.KeyFrequency<String>> snapshot = shk.topKeys();
               assertThat(snapshot).as("snapshot %d must not be empty", s).isNotEmpty();
               assertThat(snapshot)
                     .as("snapshot %d must hold the hot key with accesses", s)
                     .anySatisfy(kf -> {
                        assertThat(kf.key()).isEqualTo("hot");
                        assertThat(kf.count()).isGreaterThan(0);
                     });
               for (HeavyKeeper.KeyFrequency<String> kf : snapshot) {
                  assertThat(kf.key()).satisfiesAnyOf(
                        k -> assertThat(k).isEqualTo("hot"),
                        k -> assertThat(k).startsWith("noise-")
                  );
                  // A noise key listed by a shard in the first pass can be evicted before the second pass counts it,
                  // so a zero count is legitimate. A negative count means a torn read.
                  assertThat(kf.count()).as("count of %s in snapshot %d", kf.key(), s).isNotNegative();
               }
            }
         } finally {
            // Release the writers even when an assertion fails, so they never spin forever.
            readerDone.set(true);
         }
         return null;
      }));

      startLatch.countDown();
      for (Future<?> f : futures) {
         f.get(30, TimeUnit.SECONDS);
      }

      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).isNotEmpty();
      assertThat(top.get(0).key()).isEqualTo("hot");
   }

   @Test
   public void testResetDuringConcurrentWrites() throws Exception {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 8, 7, 0.9, HASH);
      int writerCount = 8;
      int opsBeforeReset = 10_000;
      int opsAfterReset = 1_000;
      executor = Executors.newFixedThreadPool(writerCount + 1);

      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch writersWarmedUp = new CountDownLatch(writerCount);
      AtomicBoolean resetDone = new AtomicBoolean();
      List<Future<?>> futures = new ArrayList<>(writerCount + 1);

      for (int t = 0; t < writerCount; t++) {
         futures.add(executor.submit(() -> {
            startLatch.await();
            for (int i = 0; i < opsBeforeReset; i++) {
               shk.tryAdd("before-" + (i % 50));
            }

            // Every "before-" key is recorded before this count down, so before the reset starts.
            writersWarmedUp.countDown();

            // Keep writing while the reset runs, so the reset overlaps the writes on every shard.
            int i = 0;
            while (!resetDone.get()) {
               shk.tryAdd("after-" + (i++ % 50));
            }

            // A fixed tail after the reset, so the keeper holds keys whatever the timing.
            for (int j = 0; j < opsAfterReset; j++) {
               shk.tryAdd("after-" + (j % 50));
            }
            return null;
         }));
      }

      futures.add(executor.submit(() -> {
         try {
            assertThat(writersWarmedUp.await(10, TimeUnit.SECONDS)).isTrue();
            shk.reset();
         } finally {
            // Release the writers even when the wait fails, so they never spin forever.
            resetDone.set(true);
         }
         return null;
      }));

      startLatch.countDown();
      for (Future<?> f : futures) {
         f.get(30, TimeUnit.SECONDS);
      }

      // A reset that skips a shard, or does nothing, leaves "before-" keys with far higher counts.
      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).as("keys written after the reset must be tracked").isNotEmpty();
      assertThat(top).allSatisfy(kf -> {
         assertThat(kf.key()).as("only keys written after the reset survive it").startsWith("after-");
         assertThat(kf.count()).isGreaterThan(0);
      });
   }

   @Test
   public void testKeySpreadAcrossShardsReportedWithFullCount() throws Exception {
      int k = 2;
      // A sketch wide enough that the few keys per shard never collide, so every count is exact.
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(k, 1024, 7, 0.9, MurmurHash3.getInstance()::MurmurHash3_x64_64);
      List<ShardThread> threads = threadsOnDistinctShards();

      // The first shard sees the shared key as its hottest key.
      runOn(threads.get(0), () -> {
         addTimes(shk, "shared", 300);
         addTimes(shk, "local-0", 50);
      });

      // Every other shard has two hotter keys of its own, so the shared key never enters that shard's top-k.
      for (int i = 1; i < threads.size(); i++) {
         int shard = i;
         runOn(threads.get(i), () -> {
            addTimes(shk, "local-" + shard + "-a", 100);
            addTimes(shk, "local-" + shard + "-b", 90);
            addTimes(shk, "shared", 60);
         });
      }

      // The shared key has 300 + 7 * 60 = 720 of 2100 accesses, above 2100 / (k + 1) = 700.
      // Such a key must be reported with the accesses from every shard, not only from the shards listing it.
      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).hasSizeLessThanOrEqualTo(k);
      assertThat(top.get(0).key()).isEqualTo("shared");
      assertThat(top.get(0).count()).isEqualTo(720);
   }

   /**
    * Records {@code times} accesses to {@code key} from the calling thread.
    *
    * @param shk   the keeper to record into
    * @param key   the key to record
    * @param times the number of accesses
    */
   private static void addTimes(StripedHeavyKeeper<String> shk, String key, int times) {
      for (int i = 0; i < times; i++) {
         shk.tryAdd(key);
      }
   }

   /**
    * Runs a task on the given thread and waits for it to finish. Tasks never overlap, so no update is dropped.
    *
    * @param thread a thread that has not been started yet
    * @param task   the work to run on {@code thread}
    * @throws InterruptedException if interrupted while waiting
    */
   private static void runOn(ShardThread thread, Runnable task) throws InterruptedException {
      thread.task = task;
      thread.start();
      thread.join();
   }

   /**
    * Creates eight unstarted threads whose ids map to eight different shards.
    * <p>
    * Shards are chosen by {@code threadId & (shardCount - 1)} and the shard count is 8, 16 or 32. Ids with
    * different remainders modulo 8 also differ modulo 16 and 32, so they land in different shards for any shard count.
    * </p>
    *
    * @return eight threads, one per remainder of the id modulo 8
    */
   private static List<ShardThread> threadsOnDistinctShards() {
      Map<Long, ShardThread> byResidue = new HashMap<>();
      while (byResidue.size() < 8) {
         ShardThread t = new ShardThread();
         byResidue.putIfAbsent(t.getId() & 7, t);
      }
      return new ArrayList<>(byResidue.values());
   }

   /**
    * A thread whose task is chosen after creation, so the test can pick threads by id before deciding what they run.
    */
   private static final class ShardThread extends Thread {
      private Runnable task;

      /**
       * Runs the task set by {@link #runOn(ShardThread, Runnable)}.
       */
      @Override
      public void run() {
         task.run();
      }
   }
}
