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
import org.infinispan.util.concurrent.BoundedConcurrentHashMapTest;
import org.testng.annotations.Test;

import java.util.Arrays;
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
public class ComparingConcurrentHashMapV8Test extends BoundedConcurrentHashMapTest {

   public void testByteArrayComputeIfAbsent() {
      byteArrayComputeIfAbsent(createComparingConcurrentMap());
   }

   public void testByteArrayComputeIfPresent() {
      byteArrayComputeIfPresent(createComparingConcurrentMap());
   }

   public void testByteArrayMerge() {
      byteArrayMerge(createComparingConcurrentMap());
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

   @Override
   protected ConcurrentMap<byte[], byte[]> createStandardConcurrentMap() {
      return new ConcurrentHashMap<byte[], byte[]>();
   }

   @Override
   protected ComparingConcurrentHashMapV8<byte[], byte[]> createComparingConcurrentMap() {
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
