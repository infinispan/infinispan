/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.util.concurrent.jdk8backported;

import org.infinispan.util.Comparing;
import org.infinispan.util.ComparingByteArray;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.AssertJUnit.*;

/**
 * Verifies that the {@link ComparingConcurrentHashMapV8}'s customizations
 * for equals and hashCode callbacks are working as expected. In other words,
 * if byte arrays are used as key/value types, the equals and hashCode
 * calculations are done based on their contents and not on their standard
 * JDK behaviour.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "util.concurrent.jdk8backported.ComparingConcurrentHashMapV8Test")
public class ComparingConcurrentHashMapV8Test {

   private static final Comparing COMPARING = new DebugComparingByteArray();

   public void testJdkMapExpectations() {
      byteArrayGet(createJdkChm(), false);
      byteArrayReplace(createJdkChm(), false);
      byteArrayPutSameValueTwice(createJdkChm(), false);
      byteArrayPutIfAbsentFail(createJdkChm(), false);
      byteArrayPutAll(createJdkChm(), 3);
      byteArrayContainsValue(createJdkChm(), false);
      byteArrayEquals(createJdkChm(), createJdkChm(), false);
      byteArrayEntryEquality(createJdkChm(), createJdkChm(), false);
      byteArrayValuesContains(createJdkChm(), false);
      byteArrayValuesRemove(createJdkChm(), false);
      byteArrayKeySetContains(createJdkChm(), false);
      byteArrayKeySetRemove(createJdkChm(), false);
   }

   public void testByteArrayGet() {
      byteArrayGet(createComparingChm(), true);
   }

   public void testByteArrayReplace() {
      byteArrayReplace(createComparingChm(), true);
   }

   public void testByteArrayPutSameValueTwice() {
      byteArrayPutSameValueTwice(createComparingChm(), true);
   }

   public void testByteArrayPutIfAbsentFail() {
      byteArrayPutIfAbsentFail(createComparingChm(), true);
   }

   public void testByteArrayPutAll() {
      byteArrayPutAll(createComparingChm(), 2);
   }

   public void testByteArrayContainsValue() {
      byteArrayContainsValue(createComparingChm(), true);
   }

   public void testByteArrayEquals() {
      byteArrayEquals(createComparingChm(), createComparingChm(), true);
   }

   public void testByteArrayEntryEquality() {
      byteArrayEntryEquality(createComparingChm(), createComparingChm(), true);
   }

   public void testByteArrayValuesContains() {
      byteArrayValuesContains(createComparingChm(), true);
   }

   public void testByteArrayValuesRemove() {
      byteArrayValuesRemove(createComparingChm(), true);
   }

   public void testByteArrayEntrySetContains() {
      byteArrayEntrySetContains(createComparingChm(), createComparingChm(), true);
   }

   public void testByteArrayEntrySetRemove() {
      byteArrayEntrySetRemove(createComparingChm(), createComparingChm(), true);
   }

   public void testByteArrayKeySetContains() {
      byteArrayKeySetContains(createComparingChm(), true);
   }

   public void testByteArrayKeySetRemove() {
      byteArrayKeySetRemove(createComparingChm(), true);
   }

   public void testByteArrayComputeIfAbsent() {
      byteArrayComputeIfAbsent(createComparingChm());
   }

   public void testByteArrayComputeIfPresent() {
      byteArrayComputeIfPresent(createComparingChm());
   }

   public void testByteArrayMerge() {
      byteArrayMerge(createComparingChm());
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

   // TODO: test merge

   private void byteArrayGet(
         ConcurrentMap<byte[], byte[]> map, boolean expectFound) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupKey = {1, 2, 3}; // on purpose, different instance required
      if (expectFound)
         assertTrue(String.format(
               "Expected key=%s to return value=%s", str(lookupKey), str(value)),
               Arrays.equals(value, map.get(lookupKey)));
      else
         assertNull(map.get(lookupKey));
   }

   private void byteArrayReplace(
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

   private void byteArrayPutSameValueTwice(
         ConcurrentMap<byte[], byte[]> map, boolean expectFound) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] putKey = {1, 2, 3}; // on purpose, different instance required
      byte[] sameValue = {4, 5, 6}; // on purpose, different instance required
      if (expectFound)
         assertTrue(String.format(
               "Expected putting %s again on key=%s to return value=%s",
               str(sameValue), str(putKey), str(value)),
               Arrays.equals(value, map.put(putKey, sameValue)));
      else
         assertNull(map.put(putKey, sameValue));
   }

   private void byteArrayPutIfAbsentFail(
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

   private void byteArrayPutAll(
         ConcurrentMap<byte[], byte[]> map, int expectCount) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      Map<byte[], byte[]> data = new HashMap<byte[], byte[]>();
      data.put(new byte[]{1, 2, 3}, new byte[]{7, 8, 9});
      data.put(new byte[]{11, 22, 33}, new byte[]{44, 55, 66});

      map.putAll(data);
      assertEquals(expectCount, map.size());
   }

   private void byteArrayContainsValue(
         ConcurrentMap<byte[], byte[]> map, boolean expectFound) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupValue = {4, 5, 6}; // on purpose, different instance required
      if (expectFound)
         assertTrue(String.format(
               "Expected value=%s lookup to return value=%s", str(lookupValue), str(value)),
               map.containsValue(lookupValue));
      else
         assertFalse(map.containsValue(lookupValue));
   }

   private void byteArrayEquals(ConcurrentMap<byte[], byte[]> map1,
         ConcurrentMap<byte[], byte[]> map2, boolean expectEquals) {
      byte[] key = {11, 22, 33};
      map1.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      map1.put(key, new byte[]{7, 8, 9});
      map2.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      map2.put(key, new byte[]{7, 8, 9});

      if (expectEquals) {
         assertEquals(map1, map2);
         assertEquals(map1.hashCode(), map2.hashCode());
      } else
         assertFalse(String.format(
               "Expected map1=%s to be distinct to map2=%s", map1.toString(), map2.toString()),
               map1.equals(map2));
   }

   private void byteArrayEntryEquality(ConcurrentMap<byte[], byte[]> map1,
         ConcurrentMap<byte[], byte[]> map2, boolean expectEquals) {
      map1.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      map2.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});

      Map.Entry<byte[], byte[]> entry1 = map1.entrySet().iterator().next();
      Map.Entry<byte[], byte[]> entry2 = map2.entrySet().iterator().next();
      if (expectEquals) {
         assertEquals(entry1, entry2);
         assertEquals(entry1.hashCode(), entry2.hashCode());
      } else {
         assertFalse(String.format(
               "Expected entry1=%s to be distinct to entry2=%s", entry1, entry2),
               entry1.equals(entry2));
      }
   }

   private void byteArrayValuesContains(
         ConcurrentMap<byte[], byte[]> map, boolean expectContains) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] containsValue = {4, 5, 6}; // on purpose, different instance required
      Collection<byte[]> values = map.values();
      if (expectContains)
         assertTrue(String.format(
               "Expected value=%s to be contained in values=%s", str(containsValue), values),
               values.contains(containsValue));
      else
         assertFalse(values.contains(containsValue));
   }

   private void byteArrayValuesRemove(
         ConcurrentMap<byte[], byte[]> map, boolean expectRemove) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] removeValue = {4, 5, 6}; // on purpose, different instance required
      Collection<byte[]> values = map.values();
      if (expectRemove)
         assertTrue(String.format(
               "Expected value=%s to be removed from values=%s", str(removeValue), values),
               values.remove(removeValue));
      else
         assertFalse(values.remove(removeValue));
   }

   private void byteArrayEntrySetContains(ConcurrentMap<byte[], byte[]> map1,
         ConcurrentMap<byte[], byte[]> map2, boolean expectContains) {
      map1.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      map2.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      Set<Map.Entry<byte[],byte[]>> entries1 = map1.entrySet();
      Set<Map.Entry<byte[],byte[]>> entries2 = map2.entrySet();
      Map.Entry<byte[], byte[]> entry1 = entries1.iterator().next();
      if (expectContains)
         assertTrue(String.format(
               "Expected entry=%s to be contained in entries=%s", entry1, entries2),
               entries2.contains(entry1));
      else
         assertFalse(entries2.contains(entry1));
   }

   private void byteArrayEntrySetRemove(ConcurrentMap<byte[], byte[]> map1,
         ConcurrentMap<byte[], byte[]> map2, boolean expectRemove) {
      map1.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      map2.put(new byte[]{1, 2, 3}, new byte[]{4, 5, 6});
      Set<Map.Entry<byte[],byte[]>> entries1 = map1.entrySet();
      Set<Map.Entry<byte[],byte[]>> entries2 = map2.entrySet();
      Map.Entry<byte[], byte[]> entry1 = entries1.iterator().next();
      if (expectRemove)
         assertTrue(String.format(
               "Expected entry=%s to be removed from entries=%s", entry1, entries2),
               entries2.remove(entry1));
      else
         assertFalse(entries2.remove(entry1));
   }

   private void byteArrayKeySetContains(
         ConcurrentMap<byte[], byte[]> map, boolean expectContains) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] containsKey = {1, 2, 3}; // on purpose, different instance required
      Set<byte[]> keys = map.keySet();
      if (expectContains)
         assertTrue(String.format(
               "Expected key=%s to be contained in keys=%s", str(containsKey), keys),
               keys.contains(containsKey));
      else
         assertFalse(keys.contains(containsKey));
   }

   private void byteArrayKeySetRemove(
         ConcurrentMap<byte[], byte[]> map, boolean expectRemove) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] removeKey = {1, 2, 3}; // on purpose, different instance required
      Set<byte[]> keys = map.keySet();
      if (expectRemove)
         assertTrue(String.format(
               "Expected value=%s to be removed from keys=%s", str(removeKey), keys),
               keys.remove(removeKey));
      else
         assertFalse(keys.remove(removeKey));
   }

   private void byteArrayComputeIfAbsent(
         ComparingConcurrentHashMapV8<byte[], byte[]> map) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      byte[] computeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = map.computeIfAbsent(
            computeKey, new ComparingConcurrentHashMapV8.Fun<byte[], byte[]>() {
         @Override
         public byte[] apply(byte[] bytes) {
            return new byte[]{7, 8, 9};
         }
      });

      // Old value should be present
      assertTrue(String.format(
            "Expected value=%s to be returned by operation, instead returned value=%s",
                  str(value), str(newValue)),
            Arrays.equals(newValue, value));
   }

   private void byteArrayComputeIfPresent(
         ComparingConcurrentHashMapV8<byte[], byte[]> map) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      byte[] computeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = map.computeIfPresent(computeKey,
            new ComparingConcurrentHashMapV8.BiFun<byte[], byte[], byte[]>() {
               @Override
               public byte[] apply(byte[] bytes, byte[] bytes2) {
                  return new byte[]{7, 8, 9};
               }
            }
      );

      byte[] expectedValue = {7, 8, 9};
      assertTrue(String.format(
            "Expected value=%s to be returned by operation, instead returned value=%s",
            str(expectedValue), str(newValue)),
            Arrays.equals(newValue, expectedValue));
   }

   private void byteArrayMerge(
         ComparingConcurrentHashMapV8<byte[], byte[]> map) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      byte[] computeKey = {1, 2, 3}; // on purpose, different instance required
      byte[] newValue = map.merge(computeKey, new byte[]{},
         new ComparingConcurrentHashMapV8.BiFun<byte[], byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] bytes, byte[] bytes2) {
               return new byte[]{7, 8, 9};
            }
         }
      );

      // Old value should be present
      byte[] expectedValue = {7, 8, 9};
      assertTrue(String.format(
            "Expected value=%s to be returned by operation, instead returned value=%s",
            str(expectedValue), str(newValue)),
            Arrays.equals(newValue, expectedValue));
   }

   private ConcurrentMap<byte[], byte[]> createJdkChm() {
      return new ConcurrentHashMap<byte[], byte[]>();
   }

   private ComparingConcurrentHashMapV8<byte[], byte[]> createComparingChm() {
      return new ComparingConcurrentHashMapV8<byte[], byte[]>(COMPARING);
   }

//
//   TODO: Enable test when Comparing.compare() function for a byte[] has been created and tree hash bins can be used
//
//   private ComparingConcurrentHashMapV8<byte[], byte[]> createComparingTreeHashBinsForceChm() {
//      return new ComparingConcurrentHashMapV8<byte[], byte[]>(2, new SameHashByteArray(), COMPARING);
//   }

   private String str(byte[] array) {
      // Ignore IDE warning about hashCode() call!!
      return array != null
            ? Arrays.toString(array) + "@" + Integer.toHexString(array.hashCode())
            : "null";
   }

   private static class DebugComparingByteArray implements Comparing {

      final Comparing delegate = ComparingByteArray.INSTANCE;

      @Override
      public int hashCode(Object obj) {
         return delegate.hashCode(obj);
      }

      @Override
      public boolean equals(Object obj, Object otherObj) {
         return delegate.equals(obj, otherObj);
      }

      @Override
      public String toString(Object obj) {
         return delegate.toString(obj) + "@" + Integer.toHexString(obj.hashCode());
      }

      @Override
      public boolean isComparable(Object obj) {
         return delegate.isComparable(obj);
      }

      @Override
      public int compare(Object obj, Object otherObj) {
         return delegate.compare(obj, otherObj);
      }

   }

//
//   TODO: Enable test when Comparing.compare() function for a byte[] has been created and tree hash bins can be used
//
//   private static class SameHashByteArray extends DebugComparingByteArray {
//      @Override
//      public int hashCode(Object obj) {
//         return 1;
//      }
//   }

}
