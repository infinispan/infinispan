package org.infinispan.commons.util.concurrent.jdk8backported;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.Eviction;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.EvictionListener;
import org.infinispan.commons.util.concurrent.jdk8backported.BoundedEquivalentConcurrentHashMapV8.NullEvictionListener;
import org.infinispan.util.EquivalentHashMapTest;
import org.testng.annotations.Test;

/**
 * Tests bounded concurrent hash map V8 logic against the JDK ConcurrentHashMap.
 *
 * @author William Burns
 * @since 7.1
 */
@Test(groups = "functional", testName = "util.concurrent.BoundedConcurrentHashMapTest")
public class BoundedEquivalentConcurrentHashMapV8Test extends EquivalentHashMapTest {

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
      return new ConcurrentHashMap<byte[], byte[]>();
   }

   protected ConcurrentMap<byte[], byte[]> createComparingConcurrentMap() {
      return new BoundedEquivalentConcurrentHashMapV8<byte[], byte[]>(64, EQUIVALENCE, EQUIVALENCE);
   }

   public void testLRUCacheHits() throws InterruptedException
   {
      final int COUNT_PER_THREAD = 100000;
      final int THREADS = 10;
      final int COUNT = COUNT_PER_THREAD * THREADS;

      final EvictionListener<Integer, Integer> l = new NullEvictionListener<Integer, Integer>() {
         @Override
         public void onEntryChosenForEviction(Entry<Integer, Integer> entry) {
            assertEquals(COUNT, entry.getValue().intValue());
         }
      };

      final Map<Integer, Integer> bchm = createMap(COUNT + 1, Eviction.LRU, l);

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
            };
         };
      }
      for (int i = 0; i < THREADS; i++)
         threads[i].start();
      for (int i = 0; i < THREADS; i++)
         threads[i].join();

      // adding one more entry must evict COUNT
      bchm.put(COUNT + 1, COUNT + 1);
   }

   public void testLRUHitWhenHead() {
      final BoundedEquivalentConcurrentHashMapV8<Integer, Integer> bchm = 
            createMap(2, Eviction.LRU);
      bchm.put(0, 0);
      bchm.put(1, 1);
      
      assertEquals(0, bchm.get(0).intValue());
   }

   public void testLRUHitWhenTail() {
      final BoundedEquivalentConcurrentHashMapV8<Integer, Integer> bchm = 
            createMap(2, Eviction.LRU);
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

   /**
    * Test that constantly inserts values into the map for different tables, which will
    * hopefully detect a deadlock when a new values is inserted in 1 table that has to
    * evict the value from another table
    * @throws TimeoutException 
    * @throws BrokenBarrierException 
    * @throws InterruptedException 
    */
   public void testDeadlockEvictingElementInOtherTable() throws InterruptedException,
         BrokenBarrierException, TimeoutException {
      Map<HashCodeControlled, Object> bchm = 
            createMap(2, Eviction.LRU);
      CyclicBarrier barrier = new CyclicBarrier(3);
      ExecutorService service = Executors.newFixedThreadPool(2);
      
      
      try {
         // Now we do a bunch of puts trying to interleave them
         for (int i = 0; i < 10000; i++) {
            service.submit(new HashCodeControlledPutCallable(0, bchm, barrier));
            service.submit(new HashCodeControlledPutCallable(1, bchm, barrier));
            barrier.await(10, TimeUnit.SECONDS);
         }
      } finally {
         service.shutdownNow();
      }
   }

   public void testLRUEvictionOrder() throws InterruptedException
   {
      final EvictionListener<Integer, Integer> l = new NullEvictionListener<Integer, Integer>() {
         private int index = 0;
         private int entries[] = new int[]{1, 0};
         @Override
         public void onEntryChosenForEviction(Entry<Integer, Integer> entry) {
            assertEquals(entries[index++], entry.getValue().intValue());
         }
      };

      BoundedEquivalentConcurrentHashMapV8<Integer, Integer> bchm = createMap(3, Eviction.LRU, l);

      bchm.put(0, 0); // LRU: 0
      bchm.put(1, 1); // LRU: 0, 1
      bchm.get(0);    // LRU: 1, 0
      bchm.put(2, 2); // LRU: 1, 0, 2
      bchm.put(3, 3); // evict 1, LRU: 0, 2, 3
      bchm.put(4, 4); // evict 0, LRU: 2, 3, 4
   }

   private <K, V> BoundedEquivalentConcurrentHashMapV8<K, V> createMap(int maxSize,
         Eviction eviction) {
      return createMap(maxSize, eviction, 
            BoundedEquivalentConcurrentHashMapV8.getNullEvictionListener());
   }

   private <K, V> BoundedEquivalentConcurrentHashMapV8<K, V> createMap(int maxSize,
         Eviction eviction, EvictionListener<? super K, ? super V> listener) {
      return new BoundedEquivalentConcurrentHashMapV8<K, V>(
            maxSize, maxSize >> 1, eviction, listener, 
            AnyEquivalence.getInstance(), AnyEquivalence.getInstance());
   }
}
