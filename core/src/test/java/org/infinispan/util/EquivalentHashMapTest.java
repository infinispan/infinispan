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

package org.infinispan.util;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.testng.AssertJUnit.*;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "util.EquivalentHashMapTest")
public class EquivalentHashMapTest {

   protected static final Equivalence<byte[]> EQUIVALENCE =
         new DebugByteArrayEquivalence();

   public void testJdkMapExpectations() {
      byteArrayGet(createStandardConcurrentMap(), false);
      byteArrayContainsKey(createStandardConcurrentMap(), false);
      byteArrayRemove(createStandardConcurrentMap(), false);
//      byteArrayConditionalRemove(createStandardConcurrentMap(), false);
//      byteArrayReplace(createStandardConcurrentMap(), false);
      byteArrayPutSameValueTwice(createStandardConcurrentMap(), false);
//      byteArrayPutIfAbsentFail(createStandardConcurrentMap(), false);
      byteArrayPutAll(createStandardConcurrentMap(), 3);
      byteArrayContainsValue(createStandardConcurrentMap(), false);
      byteArrayEquals(createStandardConcurrentMap(), createStandardConcurrentMap(), false);
      byteArrayEntryEquality(createStandardConcurrentMap(), createStandardConcurrentMap(), false);
      byteArrayValuesContains(createStandardConcurrentMap(), false);
      byteArrayValuesRemove(createStandardConcurrentMap(), false);
      byteArrayKeySetContains(createStandardConcurrentMap(), false);
      byteArrayKeySetRemove(createStandardConcurrentMap(), false);
   }

   public void testByteArrayGet() {
      byteArrayGet(createComparingConcurrentMap(), true);
   }

   public void testByteArrayContainsKey() {
      byteArrayContainsKey(createComparingConcurrentMap(), true);
   }

   public void testByteArrayRemove() {
      byteArrayRemove(createComparingConcurrentMap(), true);
   }

//   public void testByteArrayConditionalRemove() {
//      byteArrayConditionalRemove(createComparingConcurrentMap(), true);
//   }
//
//   public void testByteArrayReplace() {
//      byteArrayReplace(createComparingConcurrentMap(), true);
//   }

   public void testByteArrayPutSameValueTwice() {
      byteArrayPutSameValueTwice(createComparingConcurrentMap(), true);
   }

//   public void testByteArrayPutIfAbsentFail() {
//      byteArrayPutIfAbsentFail(createComparingConcurrentMap(), true);
//   }

   public void testByteArrayPutAll() {
      byteArrayPutAll(createComparingConcurrentMap(), 2);
   }

   public void testByteArrayContainsValue() {
      byteArrayContainsValue(createComparingConcurrentMap(), true);
   }

   public void testByteArrayEquals() {
      byteArrayEquals(createComparingConcurrentMap(), createComparingConcurrentMap(), true);
   }

   public void testByteArrayEntryEquality() {
      byteArrayEntryEquality(createComparingConcurrentMap(), createComparingConcurrentMap(), true);
   }

   public void testByteArrayValuesContains() {
      byteArrayValuesContains(createComparingConcurrentMap(), true);
   }

   public void testByteArrayValuesRemove() {
      byteArrayValuesRemove(createComparingConcurrentMap(), true);
   }

   public void testByteArrayEntrySetContains() {
      byteArrayEntrySetContains(createComparingConcurrentMap(), createComparingConcurrentMap(), true);
   }

   public void testByteArrayEntrySetRemove() {
      byteArrayEntrySetRemove(createComparingConcurrentMap(), createComparingConcurrentMap(), true);
   }

   public void testByteArrayKeySetContains() {
      byteArrayKeySetContains(createComparingConcurrentMap(), true);
   }

   public void testByteArrayKeySetRemove() {
      byteArrayKeySetRemove(createComparingConcurrentMap(), true);
   }

   protected void byteArrayGet(
         Map<byte[], byte[]> map, boolean expectFound) {
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

   protected void byteArrayContainsKey(
         Map<byte[], byte[]> map, boolean expectFound) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupKey = {1, 2, 3}; // on purpose, different instance required
      if (expectFound)
         assertTrue(String.format(
               "Expected key=%s to be in collection", str(lookupKey)),
               map.containsKey(lookupKey));
      else
         assertNull(map.get(lookupKey));
   }

   protected void byteArrayRemove(
         Map<byte[], byte[]> map, boolean expectRemove) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] removeKey = {1, 2, 3}; // on purpose, different instance required
      if (expectRemove)
         assertTrue(String.format(
               "Expected key=%s to be removed", str(removeKey)),
               Arrays.equals(value, map.remove(removeKey)));
      else
         assertNull(map.get(removeKey));
   }

//   protected void byteArrayConditionalRemove(
//         Map<byte[], byte[]> map, boolean expectRemove) {
//      byte[] key = {1, 2, 3};
//      byte[] value = {4, 5, 6};
//      map.put(key, value);
//      byte[] removeKey = {1, 2, 3}; // on purpose, different instance required
//      byte[] removeValue = {4, 5, 6}; // on purpose, different instance required
//      if (expectRemove)
//         assertTrue(String.format(
//               "Expected key=%s to be removed", str(removeKey)),
//               map.remove(removeKey, removeValue));
//      else
//         assertNull(map.get(removeKey));
//   }

//   protected void byteArrayReplace(
//         ConcurrentMap<byte[], byte[]> map, boolean expectReplaced) {
//      byte[] key = {1, 2, 3};
//      byte[] value = {4, 5, 6};
//      map.put(key, value);
//      byte[] lookupKey = {1, 2, 3};
//      byte[] oldValue = {4, 5, 6}; // on purpose, different instance required
//      byte[] newValue = {7, 8, 9}; // on purpose, different instance required
//      boolean replaced = map.replace(lookupKey, oldValue, newValue);
//      if (expectReplaced)
//         assertTrue(String.format(
//               "Expected key=%s replace of oldValue=%s with newValue=%s to work",
//               str(lookupKey), str(oldValue), str(newValue)), replaced);
//      else
//         assertFalse(replaced);
//   }

   protected void byteArrayPutSameValueTwice(
         Map<byte[], byte[]> map, boolean expectFound) {
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

//   protected void byteArrayPutIfAbsentFail(
//         ConcurrentMap<byte[], byte[]> map, boolean expectFail) {
//      byte[] key = {1, 2, 3};
//      byte[] value = {4, 5, 6};
//      map.put(key, value);
//      byte[] putKey = {1, 2, 3}; // on purpose, different instance required
//      byte[] newValue = {7, 8, 9};
//      byte[] previous = map.putIfAbsent(putKey, newValue);
//      if (expectFail)
//         assertTrue(String.format(
//               "Expected putIfAbsent for key=%s to fail", str(putKey)),
//               Arrays.equals(value, previous));
//      else
//         assertNull(previous);
//   }

   protected void byteArrayPutAll(
         Map<byte[], byte[]> map, int expectCount) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);

      Map<byte[], byte[]> data = new HashMap<byte[], byte[]>();
      data.put(new byte[]{1, 2, 3}, new byte[]{7, 8, 9});
      data.put(new byte[]{11, 22, 33}, new byte[]{44, 55, 66});

      map.putAll(data);
      assertEquals(expectCount, map.size());
   }

   protected void byteArrayContainsValue(
         Map<byte[], byte[]> map, boolean expectFound) {
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

   protected void byteArrayEquals(Map<byte[], byte[]> map1,
         Map<byte[], byte[]> map2, boolean expectEquals) {
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

   protected void byteArrayEntryEquality(Map<byte[], byte[]> map1,
         Map<byte[], byte[]> map2, boolean expectEquals) {
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

   protected void byteArrayValuesContains(
         Map<byte[], byte[]> map, boolean expectContains) {
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

   protected void byteArrayValuesRemove(
         Map<byte[], byte[]> map, boolean expectRemove) {
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

   protected void byteArrayEntrySetContains(Map<byte[], byte[]> map1,
         Map<byte[], byte[]> map2, boolean expectContains) {
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

   protected void byteArrayEntrySetRemove(Map<byte[], byte[]> map1,
         Map<byte[], byte[]> map2, boolean expectRemove) {
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

   protected void byteArrayKeySetContains(
         Map<byte[], byte[]> map, boolean expectContains) {
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

   protected void byteArrayKeySetRemove(
         Map<byte[], byte[]> map, boolean expectRemove) {
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

   protected Map<byte[], byte[]> createStandardConcurrentMap() {
      return new HashMap<byte[], byte[]>();
   }

   protected Map<byte[], byte[]> createComparingConcurrentMap() {
      return new EquivalentHashMap<byte[], byte[]>(EQUIVALENCE, EQUIVALENCE);
   }

   protected String str(byte[] array) {
      // Ignore IDE warning about hashCode() call!!
      return array != null
            ? Arrays.toString(array) + "@" + Integer.toHexString(array.hashCode())
            : "null";
   }

   private static class DebugByteArrayEquivalence implements Equivalence<byte[]> {

      final Equivalence<byte[]> delegate = ByteArrayEquivalence.INSTANCE;

      @Override
      public int hashCode(Object obj) {
         return delegate.hashCode(obj);
      }

      @Override
      public boolean equals(byte[] obj, Object otherObj) {
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

}
