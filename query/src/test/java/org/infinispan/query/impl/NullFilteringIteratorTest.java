package org.infinispan.query.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.testng.annotations.Test;

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
