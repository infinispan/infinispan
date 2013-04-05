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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "util.EquivalentHashSetTest")
public class EquivalentHashSetTest {

   protected static final Equivalence<byte[]> EQUIVALENCE =
         new DebugByteArrayEquivalence();

   public void testJdkMapExpectations() {
      byteArrayContainsKey(createStandardSet(), false);
      byteArrayRemove(createStandardSet(), false);
      byteArrayAddSameValueTwice(createStandardSet(), false);
      byteArrayAddAll(createStandardSet(), 3);
      byteArrayContainsAll(createStandardSet(), false);
      byteArrayEquals(createStandardSet(), createStandardSet(), false);
      byteArrayIteratorRemove(createStandardSet(), true);
   }

   public void testByteArrayContainsKey() {
      byteArrayContainsKey(createEquivalentSet(), true);
   }

   public void testByteArrayRemove() {
      byteArrayRemove(createEquivalentSet(), true);
   }

   public void testByteArrayAddSameValueTwice() {
      byteArrayAddSameValueTwice(createEquivalentSet(), true);
   }

   public void testByteArrayAddAll() {
      byteArrayAddAll(createEquivalentSet(), 2);
   }

   public void testByteArrayContainsAll() {
      byteArrayContainsAll(createEquivalentSet(), true);
   }

   public void testByteArrayEquals() {
      byteArrayEquals(createEquivalentSet(), createEquivalentSet(), true);
   }

   public void testByteArrayIteratorRemove() {
      byteArrayIteratorRemove(createEquivalentSet(), true);
   }

   protected void byteArrayContainsKey(
         Set<byte[]> set, boolean expectFound) {
      byte[] entry = {1, 2, 3};
      set.add(entry);
      byte[] lookup = {1, 2, 3}; // on purpose, different instance required
      if (expectFound)
         assertTrue(String.format(
               "Expected entry=%s to be in collection", str(lookup)),
               set.contains(lookup));
      else
         assertFalse(set.contains(lookup));
   }

   protected void byteArrayRemove(
         Set<byte[]> set, boolean expectRemove) {
      byte[] entry = {1, 2, 3};
      set.add(entry);
      byte[] remove = {1, 2, 3}; // on purpose, different instance required
      if (expectRemove)
         assertTrue(String.format(
               "Expected entry=%s to be removed", str(remove)),
               set.remove(remove));
      else
         assertFalse(set.remove(remove));
   }

   protected void byteArrayAddSameValueTwice(
         Set<byte[]> set, boolean expectFound) {
      byte[] entry = {1, 2, 3};
      set.add(entry);
      byte[] entry2 = {1, 2, 3}; // on purpose, different instance required
      if (expectFound)
         assertFalse(String.format(
               "Expected putting %s again to return true", str(entry)),
               set.add(entry2));
      else
         assertTrue(set.add(entry2));
   }

   protected void byteArrayAddAll(Set<byte[]> set, int expectCount) {
      byte[] entry = {1, 2, 3};
      set.add(entry);

      Set<byte[]> data = new HashSet<byte[]>();
      data.add(new byte[]{1, 2, 3});
      data.add(new byte[]{11, 22, 33});

      set.addAll(data);
      assertEquals(expectCount, set.size());
   }

   protected void byteArrayContainsAll(
         Set<byte[]> set, boolean expectFound) {
      byte[] entry = {1, 2, 3};
      set.add(entry);

      Set<byte[]> data = new HashSet<byte[]>();
      data.add(new byte[]{1, 2, 3});

      if (expectFound)
         assertTrue(set.containsAll(data));
      else
         assertFalse(set.containsAll(data));
   }

   protected void byteArrayEquals(Set<byte[]> set1,
         Set<byte[]> set2, boolean expectEquals) {
      set1.add(new byte[]{1, 2, 3});
      set1.add(new byte[]{4, 5, 6});
      set2.add(new byte[]{1, 2, 3});
      set2.add(new byte[]{4, 5, 6});

      if (expectEquals) {
         assertEquals(set1, set2);
         assertEquals(set1.hashCode(), set2.hashCode());
      } else
         assertFalse(String.format(
               "Expected set1=%s to be distinct to set2=%s",
               set1.toString(), set2.toString()), set1.equals(set2));
   }

   protected void byteArrayIteratorRemove(Set<byte[]> set1, boolean expectRemove) {
      set1.add(new byte[]{1, 2, 3});
      Iterator<byte[]> it1 = set1.iterator();
      byte[] entry1 = it1.next();
      if (expectRemove) {
         it1.remove();
         assertTrue(set1.isEmpty());
      }
      else
         assertFalse(set1.isEmpty());
   }

   protected Set<byte[]> createStandardSet() {
      return new HashSet<byte[]>();
   }

   protected Set<byte[]> createEquivalentSet() {
      return new EquivalentHashSet<byte[]>(EQUIVALENCE);
   }

   private String str(byte[] array) {
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
