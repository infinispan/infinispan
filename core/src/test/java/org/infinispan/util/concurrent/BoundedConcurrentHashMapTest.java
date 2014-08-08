package org.infinispan.util.concurrent;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.util.EquivalentHashMapTest;
import org.infinispan.util.concurrent.BoundedConcurrentHashMap.Eviction;
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

   private void testRemovePerformance(Eviction eviction)
   {
      final int COUNT = 200000;
      Map<Integer, Integer> bchm = new BoundedConcurrentHashMap<Integer, Integer>(
            COUNT, 1, eviction, AnyEquivalence.INT, AnyEquivalence.INT);

      // fill the cache
      for (int i = 0; i < COUNT; i++)
         bchm.put(i, i);

      // force a single cache hit (so that accessQueue has a head item)
      bchm.get(0);

      // remove items
      long start = System.currentTimeMillis();
      for (int i = 1; i < COUNT; i++)
      {
         bchm.get(i);
         bchm.remove(i);

         // original version needs ~5 min for 200k entries (2h for 1M)
         // fixed version needs < 200ms for 200k (500ms for 1M)
         if (System.currentTimeMillis() - start > 5000)
            fail(eviction.name() + ": removing " + COUNT + " entries takes more than 5 seconds!");
      }
   }

   public void testLRURemovePerformance() {
      testRemovePerformance(Eviction.LRU);
   }

   public void testLIRSRemovePerformance() {
      testRemovePerformance(Eviction.LIRS);
   }
}
