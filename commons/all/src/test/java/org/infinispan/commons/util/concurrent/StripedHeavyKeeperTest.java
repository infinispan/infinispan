package org.infinispan.commons.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongBiFunction;

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
   public void testTopKeysLimitedToK() {
      int k = 5;
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(k, 8, 7, 0.9, HASH);

      for (int i = 0; i < 100; i++) {
         shk.tryAdd("key-" + i);
      }

      assertThat(shk.topKeys()).hasSizeLessThanOrEqualTo(k);
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
      int opsPerWriter = 20_000;
      executor = Executors.newFixedThreadPool(writerCount + 1);

      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch writersWarmedUp = new CountDownLatch(writerCount);
      CountDownLatch readerDidRun = new CountDownLatch(1);
      CountDownLatch readerDone = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(writerCount + 1);

      for (int t = 0; t < writerCount; t++) {
         futures.add(executor.submit(() -> {
            startLatch.await();
            for (int i = 0; i < opsPerWriter; i++) {
               shk.tryAdd("hot");
               shk.tryAdd("noise-" + (i % 500));
               if (i == opsPerWriter / 2) {
                  writersWarmedUp.countDown();
                  assertThat(readerDidRun.await(10, TimeUnit.SECONDS)).isTrue();
               }
            }
            return null;
         }));
      }

      futures.add(executor.submit(() -> {
         assertThat(writersWarmedUp.await(10, TimeUnit.SECONDS)).isTrue();

         List<HeavyKeeper.KeyFrequency<String>> snapshot = shk.topKeys();
         assertThat(snapshot).isNotNull();
         for (HeavyKeeper.KeyFrequency<String> kf : snapshot) {
            assertThat(kf.key()).satisfiesAnyOf(
                  k -> assertThat(k).isEqualTo("hot"),
                  k -> assertThat(k).startsWith("noise-")
            );
            assertThat(kf.count()).isGreaterThan(0);
         }

         readerDidRun.countDown();

         readerDone.countDown();
         return null;
      }));

      startLatch.countDown();
      for (Future<?> f : futures) {
         f.get(30, TimeUnit.SECONDS);
      }

      assertThat(readerDone.await(0, TimeUnit.SECONDS)).isTrue();

      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).isNotEmpty();
      assertThat(top.get(0).key()).isEqualTo("hot");
   }

   @Test
   public void testResetDuringConcurrentWrites() throws Exception {
      StripedHeavyKeeper<String> shk = new StripedHeavyKeeper<>(10, 8, 7, 0.9, HASH);
      int writerCount = 8;
      int opsPerWriter = 20_000;
      executor = Executors.newFixedThreadPool(writerCount + 1);

      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch writersWarmedUp = new CountDownLatch(writerCount);
      CountDownLatch resetDidRun = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>(writerCount + 1);

      for (int t = 0; t < writerCount; t++) {
         futures.add(executor.submit(() -> {
            startLatch.await();
            for (int i = 0; i < opsPerWriter; i++) {
               shk.tryAdd("key-" + (i % 50));
               if (i == opsPerWriter / 2) {
                  writersWarmedUp.countDown();
                  assertThat(resetDidRun.await(10, TimeUnit.SECONDS)).isTrue();
               }
            }
            return null;
         }));
      }

      futures.add(executor.submit(() -> {
         assertThat(writersWarmedUp.await(10, TimeUnit.SECONDS)).isTrue();

         shk.reset();

         resetDidRun.countDown();
         return null;
      }));

      startLatch.countDown();
      for (Future<?> f : futures) {
         f.get(30, TimeUnit.SECONDS);
      }

      assertThat(resetDidRun.await(0, TimeUnit.SECONDS)).isTrue();

      List<HeavyKeeper.KeyFrequency<String>> top = shk.topKeys();
      assertThat(top).isNotNull();
   }
}
