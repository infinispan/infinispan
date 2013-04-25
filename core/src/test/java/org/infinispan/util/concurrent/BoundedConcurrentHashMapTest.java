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

package org.infinispan.util.concurrent;

import org.infinispan.util.EquivalentHashMapTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.testng.AssertJUnit.*;
import static org.testng.AssertJUnit.assertTrue;

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

}
