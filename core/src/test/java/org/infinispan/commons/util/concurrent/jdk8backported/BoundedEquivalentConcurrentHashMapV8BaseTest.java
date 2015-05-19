package org.infinispan.commons.util.concurrent.jdk8backported;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.EvictionListener;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.NullEvictionListener;
import org.infinispan.util.EquivalentHashMapTest;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests bounded concurrent hash map V8 logic against the JDK ConcurrentHashMap.
 *
 * @author William Burns
 * @since 7.1
 */
@Test(groups = "functional", testName = "util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8BaseTest")
public abstract class BoundedEquivalentConcurrentHashMapV8BaseTest extends EquivalentHashMapTest {

   protected final Log log = LogFactory.getLog(getClass());

   protected abstract Eviction evictionPolicy();

   public void testJdkMapExpectations() {
      super.testJdkMapExpectations();
      byteArrayConditionalRemove(createStandardConcurrentMap(), false);
      byteArrayReplace(createStandardConcurrentMap(), false);
      byteArrayPutIfAbsentFail(createStandardConcurrentMap(), false);
   }

   public void testByteArrayConditionalRemove() {
      byteArrayConditionalRemove(createComparingConcurrentMap(), true);
   }

   public void testByteArrayReplace() {
      byteArrayReplace(createComparingConcurrentMap(), true);
   }

   public void testByteArrayPutIfAbsentFail() {
      byteArrayPutIfAbsentFail(createComparingConcurrentMap(), true);
   }

   protected void byteArrayConditionalRemove(
         ConcurrentMap<byte[], byte[]> map, boolean expectRemove) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] removeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] removeValue = {4, 5, 6}; // on purpose, different instance required
      if (expectRemove)
         assertTrue(String.format(
               "Expected key=%s to be removed", str(removeKey)),
               map.remove(removeKey, removeValue));
      else
         assertNull(map.get(removeKey));
   }

   protected void byteArrayReplace(
         ConcurrentMap<byte[], byte[]> map, boolean expectReplaced) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupKey = {1, 2, 3};
      byte[] oldValue = {4, 5, 6}; // on purpose, different instance required
      byte[] newValue = {7, 8, 9}; // on purpose, different instance required
      boolean replaced = map.replace(lookupKey, oldValue, newValue);
      if (expectReplaced)
         assertTrue(String.format(
               "Expected key=%s replace of oldValue=%s with newValue=%s to work",
               str(lookupKey), str(oldValue), str(newValue)), replaced);
      else
         assertFalse(replaced);
   }

   protected void byteArrayPutIfAbsentFail(
         ConcurrentMap<byte[], byte[]> map, boolean expectFail) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] putKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = {7, 8, 9};
      byte[] previous = map.putIfAbsent(putKey, newValue);
      if (expectFail)
         assertTrue(String.format(
               "Expected putIfAbsent for key=%s to fail", str(putKey)),
               Arrays.equals(value, previous));
      else
         assertNull(previous);
   }

   protected ConcurrentMap<byte[], byte[]> createStandardConcurrentMap() {
      return new ConcurrentHashMap<>();
   }

   protected ConcurrentMap<byte[], byte[]> createComparingConcurrentMap() {
      return new BoundedEquivalentConcurrentHashMapV8<>(64,
            evictionPolicy(), BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener(), 
            EQUIVALENCE, EQUIVALENCE);
   }

   public void testCacheGetHits() throws InterruptedException {
      final int COUNT_PER_THREAD = 10000;
      final int THREADS = 10;
      final int COUNT = COUNT_PER_THREAD * THREADS;

      final EvictionListener<Integer, Integer> l = new NullEvictionListener<Integer, Integer>() {
         @Override
         public void onEntryChosenForEviction(Entry<Integer, Integer> entry) {
            assertEquals(COUNT, entry.getValue().intValue());
         }
      };

      final Map<Integer, Integer> bchm = createMap(COUNT + 1, evictionPolicy(), l);

      // fill the cache (note: <=, i.e. including an entry for COUNT)
      for (int i = 0; i <= COUNT; i++)
         bchm.put(i, i);

      // start 10 threads, accessing all entries except COUNT in parallel
      Thread threads[] = new Thread[THREADS];
      for (int i = 0; i < THREADS; i++) {
         final int start = COUNT_PER_THREAD * i;
         final int end = start + COUNT_PER_THREAD;
         threads[i] = new Thread() {
            public void run() {
               for (int i = start; i < end; i++)
                  assertNotNull(bchm.get(i));
            }
         };
      }
      for (int i = 0; i < THREADS; i++)
         threads[i].start();
      for (int i = 0; i < THREADS; i++)
         threads[i].join();

      // adding one more entry must evict COUNT
      bchm.put(COUNT + 1, COUNT + 1);

      if (COUNT + 1!= bchm.size()) {
         System.currentTimeMillis();
      }
      assertEquals(COUNT + 1, bchm.size());
      int manualCount = 0;
      for (Entry<Integer, Integer> entry : bchm.entrySet()) {
         if (entry.getValue() != null) {
            manualCount++;
         }
      }
      if (COUNT + 1!= manualCount) {
         System.currentTimeMillis();
      }
      assertEquals(COUNT + 1, manualCount);
   }

   /*
    * This test is to verify that LIRS works properly when we have a ton of write hits.
    * That is we have a value that already exists and it was updated
    */
   public void testCacheWriteHits() throws InterruptedException, ExecutionException, TimeoutException {
      final int COUNT = 10000;
      final Map<Integer, Integer> bchm = createMap(COUNT, evictionPolicy());

      // fill the cache (note: <=, i.e. including an entry for COUNT)
      for (int i = 0; i < COUNT; i++)
         bchm.put(i, i);
      
      final int THREADS = 10;
      
      ExecutorService service = Executors.newFixedThreadPool(THREADS);
      @SuppressWarnings("unchecked")
      Future<Void>[] futures = new Future[THREADS];
      for (int i = 0; i < THREADS; ++i) {
         // We use this so we have some threads going forward with keys on hits and
         // some going backwards.
         final int offset = i / 2;
         futures[i] = service.submit(() -> {
            for (int j = 0; j < COUNT; j++ ) {
               // Each thread will be hitting different values
               int keyValue = offset * (COUNT / THREADS) + j;
               if (keyValue >= COUNT) {
                  keyValue = keyValue - COUNT;
               }
               bchm.put(keyValue, keyValue);
            }
            return null;
         });
      }
      service.shutdown();
      service.awaitTermination(10, TimeUnit.SECONDS);
      for (int i = 0; i < THREADS; ++i) {
         futures[i].get(10, TimeUnit.SECONDS);
      }
      if (COUNT != bchm.size()) {
         System.currentTimeMillis();
      }
      assertEquals(COUNT, bchm.size());
      int manualCount = 0;
      for (Entry<Integer, Integer> entry : bchm.entrySet()) {
         if (entry.getValue() != null) {
            manualCount++;
         }
      }
      if (COUNT != manualCount) {
         System.currentTimeMillis();
      }
      assertEquals(COUNT, manualCount);
   }

   /**
    * Test to make sure that when multiple writes occur both misses and hits that
    * we have the correct eviction size later
    */
   public void testCacheWriteMissAndHit() throws InterruptedException, ExecutionException, TimeoutException {
      // This has to be even
      final int THREADS = 4;
      ExecutorService service = Executors.newFixedThreadPool(THREADS);
      ExecutorCompletionService<Void> completion = new ExecutorCompletionService<>(service);
      try {
         final int COUNT = 5;
         final Map<String, String> bchm = createMap(COUNT, evictionPolicy());
         
         // How high the write will go up to
         final int WRITE_OFFSET = 6;
         
         for (int i = 0; i < THREADS / 2; ++i) {
            completion.submit(() -> {
               for (int j = 0; j < COUNT * WRITE_OFFSET; j++ ) {
                  bchm.put("a" + j, "a" + j);
               }
               return null;
            });
            completion.submit(() -> {
                  for (int j = 0; j < COUNT * WRITE_OFFSET; j++ ) {
                     bchm.put("b" + j, "b" + j);
                  }
                  return null;
            });
         }
   
         for (int i = 0; i < THREADS; ++i) {
            Future<Void> future = completion.poll(10, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }
   
         int manualCount = 0;
         for (Entry<String, String> entry : bchm.entrySet()) {
            if (entry.getValue() != null) {
               manualCount++;
            }
         }
         if (COUNT != manualCount) {
            System.currentTimeMillis();
         }
         assertEquals(COUNT, manualCount);
   
         if (COUNT != bchm.size()) {
            System.currentTimeMillis();
         }
         assertEquals(COUNT, bchm.size());
      } finally {
         service.shutdown();
         service.awaitTermination(10, TimeUnit.SECONDS);
      }
   }

   public void testHitWhenHead() {
      final Map<Integer, Integer> bchm = createMap(2, evictionPolicy());
      bchm.put(0, 0);
      bchm.put(1, 1);
      
      assertEquals(0, bchm.get(0).intValue());
   }

   public void testHitWhenMiddle() {
      final Map<Integer, Integer> bchm = createMap(10, evictionPolicy());
      bchm.put(0, 0);
      bchm.put(1, 1);
      bchm.put(2, 2);
      
      assertEquals(1, bchm.get(1).intValue());
   }

   public void testHitWhenTail() {
      final Map<Integer, Integer> bchm = createMap(2, evictionPolicy());
      bchm.put(0, 0);
      bchm.put(1, 1);
      
      assertEquals(1, bchm.get(1).intValue());
   }

   /**
    * Class that will use the provided hash code.
    * Note that this class does not override equals so only the same instance will
    * be equal to itself
    */
   static class HashCodeControlled {
      private final int hashCode;
      
      public HashCodeControlled(int hashCode) {
         this.hashCode = hashCode;
      }

      @Override
      public int hashCode() {
         return hashCode;
      }

      @Override
      public String toString() {
         return getClass().getSimpleName() + "[hash=" + hashCode + ", systemId=" + System.identityHashCode(this) + "]";
      }
   }

   static class HashCodeControlledPutCallable implements Callable<Void> {
      private final int hashCode;
      private final Map<HashCodeControlled, Object> map;
      private final CyclicBarrier barrier;

      public HashCodeControlledPutCallable(int hashCode, Map<HashCodeControlled, Object> map,
            CyclicBarrier barrier) {
         this.hashCode = hashCode;
         this.map = map;
         this.barrier = barrier;
      }

      @Override
      public Void call() throws Exception {
         barrier.await(10, TimeUnit.SECONDS);
         HashCodeControlled hcc = new HashCodeControlled(hashCode);
         map.put(hcc, hcc);
         return null;
      }
   }

   public void testSameHashCodeLotsOfWritesWithHits() throws InterruptedException, ExecutionException, TimeoutException {
      final int READ_THREADS = 6;
      final int WRITE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int READCOUNT = 20;

      ExecutorService execService = Executors.newFixedThreadPool(READ_THREADS +
            WRITE_THREADS);
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(
            execService);

      try {
         final int COUNT = INSERTIONCOUNT >> 4;
         final int hash = 23;
         final Map<HashCodeControlled, HashCodeControlled> bchm = createMap(COUNT, evictionPolicy());
   
         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  HashCodeControlled hcc = new HashCodeControlled(hash);
                  bchm.put(hcc, hcc);
               }
               return null;
            });
         }
   
         for (int i = 0; i < READ_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < READCOUNT; ++j) {
                  for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
                     HashCodeControlled key = entry.getKey();
                     bchm.get(key);
                  }
               }
               return null;
            });
         }
   
         for (int i = 0; i < WRITE_THREADS + READ_THREADS; ++i) {
            Future<Void> future = service.poll(1000, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }

         int manualCount = 0;
         for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
            assertNotNull(entry.getValue());
            assertNotSame(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, entry.getValue());
            manualCount++;
         }
         assertEquals(COUNT, manualCount);

         assertEquals(COUNT, bchm.size());
      } finally {
         execService.shutdown();
         execService.awaitTermination(10, TimeUnit.SECONDS);
      }
   }

   public void testLotsOfWritesAndFewRemovesWithLowMaxEntries() throws Exception {
      final int WRITE_THREADS = 9;
      final int REMOVE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int REMOVECOUNT = 200;

      ExecutorService execService = Executors.newFixedThreadPool(WRITE_THREADS + REMOVE_THREADS);
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(
            execService);

      try {
         final int COUNT = 4;
         final Map<HashCodeControlled, HashCodeControlled> bchm = createMap(COUNT, evictionPolicy());
   
         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  HashCodeControlled hcc = new HashCodeControlled(j);
                  bchm.put(hcc, hcc);
               }
               return null;
            });
         }
   
         for (int i = 0; i < REMOVE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < REMOVECOUNT; ++j) {
                  for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
                     HashCodeControlled key = entry.getKey();
                     bchm.remove(key);
                  }
               }
               return null;
            });
         }
   
         for (int i = 0; i < WRITE_THREADS + REMOVE_THREADS; ++i) {
            Future<Void> future = service.poll(10, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }

         int manualCount = 0;
         for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
            if (entry.getValue() != null) {
               manualCount++;
            }
         }
         if (bchm.size() != manualCount) {
            System.currentTimeMillis();
         }
         assertEquals(bchm.size(), manualCount);

         if (manualCount > COUNT) {
            assertFalse("Count " + manualCount + " should not be higher than max " + COUNT, manualCount > COUNT);
         }
      } finally {
         execService.shutdown();
         execService.awaitTermination(1000, TimeUnit.SECONDS);
      }
   }

   public void testLotsOfWritesRemovesAndReads() throws InterruptedException, ExecutionException, TimeoutException {
      final int READ_THREADS = 8;
      final int WRITE_THREADS = 3;
      final int REMOVE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int REMOVECOUNT = 200;
      final int READCOUNT = 20;

      ExecutorService execService = Executors.newFixedThreadPool(READ_THREADS +
            WRITE_THREADS + REMOVE_THREADS);
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(
            execService);

      try {
         final int COUNT = INSERTIONCOUNT >> 4;
         final int hash = 23;
         final Map<HashCodeControlled, HashCodeControlled> bchm = createMap(COUNT, evictionPolicy());
   
         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  HashCodeControlled hcc = new HashCodeControlled(hash);
                  bchm.put(hcc, hcc);
               }
               return null;
            });
         }
   
         for (int i = 0; i < READ_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < READCOUNT; ++j) {
                  for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
                     HashCodeControlled key = entry.getKey();
                     bchm.get(key);
                  }
               }
               return null;
            });
         }

         for (int i = 0; i < REMOVE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < REMOVECOUNT; ++j) {
                  for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
                     HashCodeControlled key = entry.getKey();
                     bchm.remove(key);
                  }
               }
               return null;
            });
         }
   
         for (int i = 0; i < WRITE_THREADS + READ_THREADS + REMOVE_THREADS; ++i) {
            Future<Void> future = service.poll(10, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }

         int manualCount = 0;
         for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
            if (entry.getValue() != null) {
               manualCount++;
            }
         }
         if (bchm.size() != manualCount) {
            System.currentTimeMillis();
         }
         assertEquals(bchm.size(), manualCount);

         if (manualCount > COUNT) {
            assertFalse("Count " + manualCount + " should not be higher than max " + COUNT, manualCount > COUNT);
         }
      } finally {
         execService.shutdown();
         execService.awaitTermination(10, TimeUnit.SECONDS);
      }
   }

   /**
    * Test that constantly inserts values into the map for different tables, which will
    * hopefully detect a deadlock when a new values is inserted in 1 table that has to
    * evict the value from another table
    * @throws TimeoutException 
    * @throws BrokenBarrierException 
    * @throws InterruptedException 
    * @throws ExecutionException 
    */
   public void testDeadlockEvictingElementInOtherTable() throws InterruptedException,
         BrokenBarrierException, TimeoutException, ExecutionException {
      Map<HashCodeControlled, Object> bchm = 
            createMap(2, evictionPolicy());
      CyclicBarrier barrier = new CyclicBarrier(3);
      ExecutorService service = Executors.newFixedThreadPool(2);
      
      try {
         // Now we do a bunch of puts trying to interleave them
         for (int i = 0; i < 5000; i++) {
            Future<?> f1 = service.submit(new HashCodeControlledPutCallable(0, bchm, barrier));
            Future<?> f2 = service.submit(new HashCodeControlledPutCallable(1, bchm, barrier));
            barrier.await(10, TimeUnit.SECONDS);
            f1.get(1000, TimeUnit.SECONDS);
            f2.get(10, TimeUnit.SECONDS);
            if (bchm.size() != 2) {
               assertEquals(2, bchm.size());
            }
         }
      } finally {
         service.shutdownNow();
      }
   }

   public void testDifferentHashCodeLotsOfWritesWithHits() throws Exception {
      final int READ_THREADS = 6;
      final int WRITE_THREADS = 2;
      final int INSERTIONCOUNT = 2048;
      final int READCOUNT = 20;

      ExecutorService execService = Executors.newFixedThreadPool(READ_THREADS +
            WRITE_THREADS);
      ExecutorCompletionService<Void> service = new ExecutorCompletionService<>(
            execService);

      try {
         final int COUNT = INSERTIONCOUNT >> 4;
         final int hash = 23;
         final int hash2 = 24;
         final BoundedEquivalentConcurrentHashMapV8<HashCodeControlled, HashCodeControlled> bchm = 
               createMap(COUNT, evictionPolicy());
   
         for (int i = 0; i < WRITE_THREADS; ++i) {
            service.submit(() -> {
               for (int j = 0; j < INSERTIONCOUNT; ++j) {
                  final HashCodeControlled hcc = new HashCodeControlled(j % 2 == 0 ? hash : hash2);
                  bchm.compute(hcc, (k, v) -> hcc);
               }
               return null;
            });
         }
   
         for (int i = 0; i < READ_THREADS; ++i) {
            service.submit(() -> {
               final long currentTime = System.currentTimeMillis();
               for (int j = 0; j < READCOUNT; ++j) {
                  bchm.forEach((a, b) -> {
                     // This is just so JVM doesn't optimize away
                     if (a.hashCode == (int)currentTime) {
                        System.out.println("They Matched");
                     }
                  });
               }
               return null;
            });
         }
   
         for (int i = 0; i < WRITE_THREADS + READ_THREADS; ++i) {
            Future<Void> future = service.poll(1000, TimeUnit.SECONDS);
            if (future == null) {
               throw new TimeoutException();
            }
            future.get();
         }

         int manualCount = 0;
         for (Entry<HashCodeControlled, HashCodeControlled> entry : bchm.entrySet()) {
            assertNotNull(entry.getValue());
            assertNotSame(BoundedEquivalentConcurrentHashMapV8.NULL_VALUE, entry.getValue());
            manualCount++;
         }
         assertEquals(COUNT, manualCount);

         assertEquals(COUNT, bchm.size());
      } finally {
         execService.shutdown();
         execService.awaitTermination(10, TimeUnit.SECONDS);
      }
   }

   protected <K, V> BoundedEquivalentConcurrentHashMapV8<K, V> createMap(int maxSize,
         Eviction eviction) {
      return createMap(maxSize, eviction, 
            BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener());
   }

   protected <K, V> BoundedEquivalentConcurrentHashMapV8<K, V> createMap(int maxSize,
         Eviction eviction, EvictionListener<? super K, ? super V> listener) {
      return new BoundedEquivalentConcurrentHashMapV8<K, V>(
            maxSize, maxSize >> 1, eviction, listener, 
            AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
   }
}
