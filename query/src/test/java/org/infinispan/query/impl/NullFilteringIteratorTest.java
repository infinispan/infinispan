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
package org.infinispan.query.impl;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * @author Marko Luksa
 */
@Test(groups = "unit", testName = "query.impl.NullFilteringIteratorTest")
public class NullFilteringIteratorTest {

   @Test
   public void testEmptyIteratorReturnsFalseOnHasNext() {
      NullFilteringIterator<String> iterator = getIterator();  // empty iterator
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testEmptyIteratorThrowsExceptionOnNext() {
      NullFilteringIterator<String> iterator = getIterator();  // empty iterator
      try {
         iterator.next();
         fail("Expected NoSuchElementException");
      } catch (NoSuchElementException e) {
      }
   }

   @Test
   public void testNonNullValue() {
      NullFilteringIterator<String> iterator = getIterator("foo");
      assertTrue(iterator.hasNext());
      assertEquals(iterator.next(), "foo");
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testNullValue() {
      NullFilteringIterator<String> iterator = getIterator((String) null);
      assertFalse(iterator.hasNext());
      try {
         iterator.next();
         fail("Expected NoSuchElementException");
      } catch (NoSuchElementException e) {
      }
   }

   @Test
   public void testSingleNullValue() {
      NullFilteringIterator<String> iterator = getIterator(null, "foo");
      assertTrue(iterator.hasNext());
      assertEquals(iterator.next(), "foo");
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testMultipleConsecutiveNullValues() {
      NullFilteringIterator<String> iterator = getIterator(null, null, null, "foo");
      assertTrue(iterator.hasNext());
      assertEquals(iterator.next(), "foo");
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testNullValueAtTheEnd() {
      NullFilteringIterator<String> iterator = getIterator("foo", null);
      assertTrue(iterator.hasNext());
      assertEquals(iterator.next(), "foo");
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testMultipleNullValuesAtTheEnd() {
      NullFilteringIterator<String> iterator = getIterator("foo", null, null, null);
      assertTrue(iterator.hasNext());
      assertEquals(iterator.next(), "foo");
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testNextOnly() {
      NullFilteringIterator<String> iterator = getIterator(null, "foo", null, "bar", null);
      assertEquals(iterator.next(), "foo");
      assertEquals(iterator.next(), "bar");
      try {
         iterator.next();
         fail("Expected NoSuchElementException");
      } catch (NoSuchElementException e) {
      }
   }

   @Test
   public void testHasNextDoesNotAdvanceIterator() {
      NullFilteringIterator<String> iterator = getIterator("foo");
      assertTrue(iterator.hasNext());
      assertTrue(iterator.hasNext());
      assertEquals(iterator.next(), "foo");
      assertFalse(iterator.hasNext());
   }

   private <E> NullFilteringIterator<E> getIterator(E... elements) {
      return new NullFilteringIterator<E>(Arrays.asList(elements).iterator());
   }

}