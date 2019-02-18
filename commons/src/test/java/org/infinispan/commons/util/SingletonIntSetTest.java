package org.infinispan.commons.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.junit.Test;

/**
 * @author wburns
 * @since 9.3
 */
public class SingletonIntSetTest {
   @Test
   public void testSize() {
      IntSet rs = new SingletonIntSet(3);
      assertEquals(1, rs.size());
   }

   @Test
   public void testIsEmpty() {
      IntSet rs = new SingletonIntSet(3);
      assertFalse(rs.isEmpty());
   }

   @Test
   public void testContains() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      assertFalse(sis.contains(5));
      assertTrue(sis.contains(3));
   }

   @Test
   public void testContains1() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      assertFalse(sis.contains(Integer.valueOf(5)));
      assertTrue(sis.contains(Integer.valueOf(3)));
   }

   @Test
   public void testIterator() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      PrimitiveIterator.OfInt iterator = sis.iterator();
      assertEquals(3, iterator.nextInt());
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testToArray() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      Object[] array = sis.toArray();
      assertEquals(1, array.length);
      assertEquals(3, array[0]);
   }

   @Test
   public void testToArray1() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      Object[] array = sis.toArray(new Integer[1]);
      assertEquals(1, array.length);
      assertEquals(3, array[0]);
   }

   @Test
   public void testToIntArray() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      int[] array = sis.toIntArray();

      assertArrayEquals(new int[]{3}, array);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testAdd() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      sis.add(1);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testAdd1() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      sis.add(Integer.valueOf(1));
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testSet() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      sis.set(1);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRemove() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      sis.remove(3);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRemove1() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      sis.remove(Integer.valueOf(3));
   }

   @Test
   public void testContainsAll() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      IntSet sis2 = new SingletonIntSet(3);

      assertTrue(sis.containsAll(sis2));
      assertTrue(sis2.containsAll(sis));

      IntSet sis3 = new RangeSet(4);

      assertTrue(sis3.containsAll(sis));
      assertFalse(sis.containsAll(sis3));
   }

   @Test
   public void testContainsAll1() throws Exception {
      IntSet sis = new SingletonIntSet(3);
      Set<Integer> hashSet = new HashSet<>(Arrays.asList(3));

      assertTrue(sis.containsAll(hashSet));
      assertTrue(hashSet.containsAll(sis));

      hashSet.add(0);

      assertTrue(hashSet.containsAll(sis));
      assertFalse(sis.containsAll(hashSet));
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testAddAll() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      IntSet rs = new RangeSet(5);
      sis.addAll(rs);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testAddAll1() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      SmallIntSet sis2 = new SmallIntSet();
      sis.addAll(sis2);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testAddAll2() throws Exception {
      Set<Integer> hashSet = Util.asSet(1, 4);

      IntSet sis = new SingletonIntSet(3);
      sis.addAll(hashSet);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRemoveAll() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      IntSet rs = new RangeSet(6);

      sis.removeAll(rs);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRemoveAll1() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> hashSet = new HashSet<>();

      sis.removeAll(hashSet);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRetainAll() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      IntSet rs = new RangeSet(5);

      sis.retainAll(rs);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRetainAll1() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      IntSet sis2 = new SingletonIntSet(3);

      sis.retainAll(sis2);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRetainAll2() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> hashSet = new HashSet<>();

      sis.retainAll(hashSet);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testClear() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      sis.clear();
   }

   @Test
   public void testIntStream() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      assertEquals(1, sis.intStream().count());
   }

   @Test
   public void testEquals() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      IntSet sis2 = new SingletonIntSet(4);

      // Verify equals both ways
      assertNotEquals(sis, sis2);
      assertNotEquals(sis2, sis);

      IntSet sis3 = SmallIntSet.of(3);

      assertEquals(sis3, sis);
      assertEquals(sis, sis3);
   }

   @Test
   public void testEquals1() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      SmallIntSet sis2 = new SmallIntSet();
      sis2.add(2);
      sis2.add(3);

      // Verify equals both ways
      assertNotEquals(sis, sis2);
      assertNotEquals(sis2, sis);

      sis2.remove(2);

      assertEquals(sis, sis2);
      assertEquals(sis2, sis);
   }

   @Test
   public void testEquals2() throws Exception {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(2);
      hashSet.add(3);

      // Verify equals both ways
      assertNotEquals(sis, hashSet);
      assertNotEquals(hashSet, sis);

      hashSet.remove(2);

      assertEquals(sis, hashSet);
      assertEquals(hashSet, sis);
   }

   @Test
   public void testForEachPrimitive() {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> results = new HashSet<>();

      sis.forEach((IntConsumer) results::add);

      assertEquals(1, results.size());
      assertTrue(results.contains(3));
   }

   @Test
   public void testForEachObject() {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> results = new HashSet<>();

      sis.forEach((Consumer<? super Integer>) results::add);

      assertEquals(1, results.size());
      assertTrue(results.contains(3));
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRemoveIfPrimitive() {
      IntSet sis = new SingletonIntSet(3);
      sis.removeIf((int i) -> i == 3);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testRemoveIfObject() {
      IntSet sis = new SingletonIntSet(3);
      sis.removeIf((Integer i) -> i == 3);
   }

   @Test
   public void testIntSpliteratorForEachRemaining() {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> results = new HashSet<>();

      sis.intSpliterator().forEachRemaining((IntConsumer) results::add);

      assertEquals(1, results.size());
      assertTrue(results.contains(3));
   }

   @Test
   public void testIntSpliteratorSplitTryAdvance() {
      IntSet sis = new SingletonIntSet(3);

      Set<Integer> results = new HashSet<>();

      Spliterator.OfInt spliterator = sis.intSpliterator();
      Spliterator.OfInt split = spliterator.trySplit();

      assertNull(split);

      IntConsumer consumer = results::add;

      while (spliterator.tryAdvance(consumer)) { }

      assertEquals(1, results.size());
      assertTrue(results.contains(3));
   }
}
