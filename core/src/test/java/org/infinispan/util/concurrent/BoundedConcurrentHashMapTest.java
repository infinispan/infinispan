package org.infinispan.util.concurrent;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.util.EquivalentHashMapTest;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.EvictionListener;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.NullEvictionListener;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.AssertJUnit.*;

/**
 * Tests bounded concurrent hash map logic against the JDK ConcurrentHashMap.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.concurrent.BoundedConcurrentHashMapTest")
public class BoundedConcurrentHashMapTest extends EquivalentHashMapTest {

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
      return new BoundedConcurrentHashMap<byte[], byte[]>(EQUIVALENCE, EQUIVALENCE);
   }

//
//   TODO: Enable test when Comparing.compare() function for a byte[] has been created, and hence tree based hash bins can be used.
//
//   public void testByteArrayOperationsWithTreeHashBins() {
//      // This test forces all entries to be stored under the same hash bin,
//      // kicking off different logic for comparing keys.
//      ComparingConcurrentHashMapV8<byte[], byte[]> map =
//            createComparingTreeHashBinsForceChm();
//      for (byte b = 0; b < 10; b++)
//         map.put(new byte[]{b}, new byte[]{0});
//
//      byte[] key = new byte[]{10};
//      byte[] value =  new byte[]{0};
//      assertTrue(String.format(
//            "Expected key=%s to return value=%s", str(key), str(value)),
//            Arrays.equals(value, map.get(key)));
//   }

//
//   TODO: Enable test when Comparing.compare() function for a byte[] has been created and tree hash bins can be used
//
//   private ComparingConcurrentHashMapV8<byte[], byte[]> createComparingTreeHashBinsForceChm() {
//      return new ComparingConcurrentHashMapV8<byte[], byte[]>(2, new SameHashByteArray(), COMPARING);
//   }

//
//   TODO: Enable test when Comparing.compare() function for a byte[] has been created and tree hash bins can be used
//
//   private static class SameHashByteArray extends DebugComparingByteArray {
//      @Override
//      public int hashCode(Object obj) {
//         return 1;
//      }
//   }

   public void testLRUCacheHits() throws InterruptedException
   {
      final int COUNT_PER_THREAD = 100000;
      final int THREADS = 10;
      final int COUNT = COUNT_PER_THREAD * THREADS;

      final EvictionListener<Integer, Integer> l = new NullEvictionListener<Integer, Integer>() {
         @Override
         public void onEntryChosenForEviction(Integer internalCacheEntry) {
            assertEquals(COUNT, internalCacheEntry.intValue());
         }
      };

      final Map<Integer, Integer> bchm = new BoundedConcurrentHashMap<Integer, Integer>(
            COUNT + 1, 1, Eviction.LRU, l, AnyEquivalence.INT, AnyEquivalence.INT);

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

   public void testLRUEvictionOrder() throws InterruptedException
   {
      final EvictionListener<Integer, Integer> l = new NullEvictionListener<Integer, Integer>() {
         private int index = 0;
         private int entries[] = new int[]{1, 0};
         @Override
         public void onEntryChosenForEviction(Integer internalCacheEntry) {
            assertEquals(entries[index++], internalCacheEntry.intValue());
         }
      };

      Map<Integer, Integer> bchm = new BoundedConcurrentHashMap<Integer, Integer>(
            3, 1, Eviction.LRU, l, AnyEquivalence.INT, AnyEquivalence.INT);

      bchm.put(0, 0); // LRU: 0
      bchm.put(1, 1); // LRU: 0, 1
      bchm.get(0);    // LRU: 1, 0
      bchm.put(2, 2); // LRU: 1, 0, 2
      bchm.put(3, 3); // evict 1, LRU: 0, 2, 3
      bchm.put(4, 4); // evict 0, LRU: 2, 3, 4
   }
}
