package org.infinispan.stress;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.DefaultDataContainer;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.impl.ActivationManagerStub;
import org.infinispan.eviction.impl.PassivationManagerStub;
import org.infinispan.expiration.impl.InternalExpirationManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Stress test different data containers
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(testName = "stress.DataContainerStressTest", groups = "stress",
      description = "Disabled by default, designed to be run manually.", timeOut = 15*60*1000)
public class DataContainerStressTest {
   volatile CountDownLatch latch;
   final int RUN_TIME_MILLIS = 45 * 1000; // 1 min
   final int WARMUP_TIME_MILLIS = 10 * 1000; // 10 sec
   final int num_loops = 10000;
   final int warmup_num_loops = 10000;
   boolean use_time = true;
   final int NUM_KEYS = 256;

   private static final Log log = LogFactory.getLog(DataContainerStressTest.class);

   public void testSimpleDataContainer() throws InterruptedException {
      DefaultDataContainer dc = DefaultDataContainer.unBoundedDataContainer(5000);
      initializeDefaultDataContainer(dc);
      doTest(dc);
   }

   public void testEntryBoundedDataContainer() throws InterruptedException {
      DefaultDataContainer dc = DefaultDataContainer.boundedDataContainer(5000, NUM_KEYS - NUM_KEYS / 4, false);
      initializeDefaultDataContainer(dc);
      doTest(dc);
   }

   public void testMemoryBoundedDataContainer() throws InterruptedException {
      // The key length could be 4 or 5 (90% of the time it will be 5)
      // The value length could be 6 or 7 (90% of the time it will be 7)
      DefaultDataContainer dc = DefaultDataContainer.boundedDataContainer(5000, threeQuarterMemorySize(NUM_KEYS, 5, 20), false);
      initializeDefaultDataContainer(dc);
      doTest(dc);
   }

   private void initializeDefaultDataContainer(DefaultDataContainer dc) {
      InternalEntryFactoryImpl entryFactory = new InternalEntryFactoryImpl();
      TimeService timeService = new EmbeddedTimeService();
      TestingUtil.inject(entryFactory, timeService);
      // Mockito cannot be used as it will run out of memory from keeping all the invocations, thus we use blank impls
      TestingUtil.inject(dc, (EvictionManager) (evicted, cmd) -> CompletableFutures.completedNull(),
                         new PassivationManagerStub(), entryFactory, new ActivationManagerStub(), null, timeService,
                         null, new InternalExpirationManager() {
               @Override
               public void processExpiration() {

               }

               @Override
               public boolean isEnabled() {
                  return false;
               }

               @Override
               public CompletableFuture<Boolean> entryExpiredInMemory(InternalCacheEntry entry, long currentTime,
                                                                      boolean writeOperation) {
                  return null;
               }

               @Override
               public CompletionStage<Void> handleInStoreExpirationInternal(Object key) {
                  return null;
               }

               @Override
               public CompletionStage<Void> handleInStoreExpirationInternal(MarshallableEntry marshalledEntry) {
                  return null;
               }

               @Override
               public CompletionStage<Boolean> handlePossibleExpiration(InternalCacheEntry entry, int segment,
                                                                        boolean isWrite) {
                  return null;
               }

               @Override
               public void addInternalListener(ExpirationConsumer consumer) {

               }

               @Override
               public void removeInternalListener(Object listener) {

               }
            });
   }

   private long threeQuarterMemorySize(int numKeys, int keyLength, int valueLength) {
      // We are assuming each string base takes up 36 bytes (12 for the array, 8 for the class, 8 for the object itself
      // & 4 for the inner int (this aligned is 36 bytes).
      // We assume compressed strings are not enabled (so each character is 2 bytes (UTF-16)
      // We are also ignoring alignment (which the key length and value length should be aligned to the nearest 8 bytes)
      long total = numKeys * (32 + keyLength + valueLength);
      return total - total / 4;
   }

   private void doTest(final DataContainer dc) throws InterruptedException {
      doTest(dc, true);
      doTest(dc, false);
   }

   private void doTest(final DataContainer dc, boolean warmup) throws InterruptedException {
      latch = new CountDownLatch(1);
      final byte[] keyFirstBytes = new byte[4];
      final Map<String, String> perf = new ConcurrentSkipListMap<String, String>();
      final AtomicBoolean run = new AtomicBoolean(true);
      final int actual_num_loops = warmup ? warmup_num_loops : num_loops;

      Thread getter = new Thread() {
         @Override
         public void run() {
            ThreadLocalRandom R = ThreadLocalRandom.current();
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            byte[] captureByte = new byte[1];
            byte[] key = Arrays.copyOf(keyFirstBytes, 5);
            while (use_time && run.get() || runs < actual_num_loops) {
//               if (runs % 100000 == 0) log.info("GET run # " + runs);
//               TestingUtil.sleepThread(10);
               R.nextBytes(captureByte);
               key[4] = captureByte[0];
               dc.peek(key);
               runs++;
            }
            perf.put("GET", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread putter = new Thread() {
         @Override
         public void run() {
            ThreadLocalRandom R = ThreadLocalRandom.current();
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            byte[] captureByte = new byte[1];
            byte[] key = Arrays.copyOf(keyFirstBytes, 5);
            byte[] value = new byte[20];
            Metadata metadata = new EmbeddedMetadata.Builder().build();
            while (use_time && run.get() || runs < actual_num_loops) {
//               if (runs % 100000 == 0) log.info("PUT run # " + runs);
//               TestingUtil.sleepThread(10);
               R.nextBytes(captureByte);
               key[4] = captureByte[0];
               R.nextBytes(value);
               dc.put(key, value, metadata);
               runs++;
            }
            perf.put("PUT", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread remover = new Thread() {
         @Override
         public void run() {
            ThreadLocalRandom R = ThreadLocalRandom.current();
            waitForStart();
            long start = System.nanoTime();
            int runs = 0;
            byte[] captureByte = new byte[1];
            byte[] key = Arrays.copyOf(keyFirstBytes, 5);
            while (use_time && run.get() || runs < actual_num_loops) {
//               if (runs % 100000 == 0) log.info("REM run # " + runs);
//               TestingUtil.sleepThread(10);
               R.nextBytes(captureByte);
               key[4] = captureByte[0];
               dc.remove(key);
               runs++;
            }
            perf.put("REM", opsPerMS(System.nanoTime() - start, runs));
         }
      };

      Thread[] threads = {getter, putter, remover};
      for (Thread t : threads) t.start();
      latch.countDown();

      // wait some time
      Thread.sleep(warmup ? WARMUP_TIME_MILLIS : RUN_TIME_MILLIS);
      run.set(false);
      for (Thread t : threads) t.join();
      if (!warmup) log.warnf("%s: Performance: %s", dc.getClass().getSimpleName(), perf);
   }

   private void waitForStart() {
      try {
         latch.await();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private String opsPerMS(long nanos, int ops) {
      long totalMillis = TimeUnit.NANOSECONDS.toMillis(nanos);
      if (totalMillis > 0)
         return ops / totalMillis + " ops/ms";
      else
         return "NAN ops/ms";
   }
}
