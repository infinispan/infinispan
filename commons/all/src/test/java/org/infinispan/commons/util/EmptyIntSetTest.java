package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.junit.jupiter.api.Test;

/**
 * @author wburns
 * @since 9.3
 */
public class EmptyIntSetTest {
   IntSet es = EmptyIntSet.getInstance();

   @Test
   public void testSize() {
      assertEquals(0, es.size());
   }

   @Test
   public void testIsEmpty() {
      assertTrue(es.isEmpty());
   }

   @Test
   public void testContains() throws Exception {
      assertFalse(es.contains(5));
      assertFalse(es.contains(3));
   }

   @Test
   public void testContains1() throws Exception {
      assertFalse(es.contains(Integer.valueOf(5)));
      assertFalse(es.contains(Integer.valueOf(3)));
   }

   @Test
   public void testIterator() throws Exception {
      PrimitiveIterator.OfInt iterator = es.iterator();
      assertFalse(iterator.hasNext());
   }

   @Test
   public void testToArray() throws Exception {
      Object[] array = es.toArray();
      assertEquals(0, array.length);
   }

   @Test
   public void testToArray1() throws Exception {
      Object[] array = es.toArray(new Integer[1]);
      assertEquals(1, array.length);
      assertNull(array[0]);

      array = es.toArray(new Integer[0]);
      assertEquals(0, array.length);
   }

   @Test
   public void testToIntArray() throws Exception {
      int[] array = es.toIntArray();
      assertEquals(0, array.length);
   }

   @Test
   public void testAdd() throws Exception {
      assertThrows(UnsupportedOperationException.class, () -> es.add(1));
   }

   @Test
   public void testAdd1() throws Exception {
      assertThrows(UnsupportedOperationException.class, () -> es.add(Integer.valueOf(1)));
   }

   @Test
   public void testSet() throws Exception {
      assertThrows(UnsupportedOperationException.class, () -> es.set(1));
   }

   @Test
   public void testRemove() throws Exception {
      assertThrows(UnsupportedOperationException.class, () -> es.remove(3));
   }

   @Test
   public void testRemove1() throws Exception {
      assertThrows(UnsupportedOperationException.class, () -> es.remove(Integer.valueOf(3)));
   }

   @Test
   public void testContainsAll() throws Exception {
      IntSet sis2 = new SingletonIntSet(3);

      assertFalse(es.containsAll(sis2));
      assertTrue(sis2.containsAll(es));

      IntSet sis3 = new RangeSet(0);

      assertTrue(sis3.containsAll(es));
      assertTrue(es.containsAll(sis3));
   }

   @Test
   public void testContainsAll1() throws Exception {
      Set<Integer> hashSet = new HashSet<>();

      hashSet.add(3);

      assertFalse(es.containsAll(hashSet));
      assertTrue(hashSet.containsAll(es));

      hashSet.remove(3);

      assertTrue(hashSet.containsAll(es));
      assertTrue(es.containsAll(hashSet));
   }

   @Test
   public void testAddAll() throws Exception {
      IntSet rs = new RangeSet(5);
      assertThrows(UnsupportedOperationException.class, () -> es.addAll(rs));
   }

   @Test
   public void testAddAll1() throws Exception {
      SmallIntSet sis2 = new SmallIntSet();
      assertThrows(UnsupportedOperationException.class, () -> es.addAll(sis2));
   }

   @Test
   public void testAddAll2() throws Exception {
      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(1);
      hashSet.add(4);

      assertThrows(UnsupportedOperationException.class, () -> es.addAll(hashSet));
   }

   @Test
   public void testRemoveAll() throws Exception {
      IntSet rs = new RangeSet(6);

      assertThrows(UnsupportedOperationException.class, () -> es.removeAll(rs));
   }

   @Test
   public void testRemoveAll1() throws Exception {
      Set<Integer> hashSet = Collections.emptySet();

      assertThrows(UnsupportedOperationException.class, () -> es.removeAll(hashSet));
   }

   @Test
   public void testRetainAll() throws Exception {
      IntSet rs = new RangeSet(5);

      assertThrows(UnsupportedOperationException.class, () -> es.retainAll(rs));
   }

   @Test
   public void testRetainAll1() throws Exception {
      Set<Integer> hashSet = Collections.emptySet();

      assertThrows(UnsupportedOperationException.class, () -> es.retainAll(hashSet));
   }

   @Test
   public void testClear() throws Exception {
      assertThrows(UnsupportedOperationException.class, () -> es.clear());
   }

   @Test
   public void testIntStream() throws Exception {
      assertEquals(0, es.intStream().count());
   }

   @Test
   public void testEquals() throws Exception {
      IntSet sis2 = new SingletonIntSet(4);

      // Verify equals both ways
      assertNotEquals(es, sis2);
      assertNotEquals(sis2, es);

      // This is empty, just sets the range
      IntSet sis3 = new SmallIntSet(2);

      assertEquals(sis3, es);
      assertEquals(es, sis3);
   }

   @Test
   public void testEquals1() throws Exception {
      SmallIntSet sis2 = new SmallIntSet();
      sis2.add(2);

      // Verify equals both ways
      assertNotEquals(es, sis2);
      assertNotEquals(sis2, es);

      sis2.remove(2);

      assertEquals(es, sis2);
      assertEquals(sis2, es);
   }

   @Test
   public void testEquals2() throws Exception {
      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(2);

      // Verify equals both ways
      assertNotEquals(es, hashSet);
      assertNotEquals(hashSet, es);

      hashSet.remove(2);

      assertEquals(es, hashSet);
      assertEquals(hashSet, es);
   }

   @Test
   public void testForEachPrimitive() {
      Set<Integer> results = new HashSet<>();

      es.forEach((IntConsumer) results::add);

      assertEquals(0, results.size());
   }

   @Test
   public void testForEachObject() {
      Set<Integer> results = new HashSet<>();

      es.forEach((Consumer<? super Integer>) results::add);

      assertEquals(0, results.size());
   }

   @Test
   public void testRemoveIfPrimitive() {
      assertThrows(UnsupportedOperationException.class, () -> es.removeIf((int i) -> i == 3));
   }

   @Test
   public void testRemoveIfObject() {
      assertThrows(UnsupportedOperationException.class, () -> es.removeIf((Integer i) -> i == 3));
   }

   @Test
   public void testIntSpliteratorForEachRemaining() {
      Set<Integer> results = new HashSet<>();

      es.intSpliterator().forEachRemaining((IntConsumer) results::add);

      assertEquals(0, results.size());
   }

   @Test
   public void testIntSpliteratorSplitTryAdvance() {
      Set<Integer> results = new HashSet<>();

      Spliterator.OfInt spliterator = es.intSpliterator();
      Spliterator.OfInt split = spliterator.trySplit();

      assertNull(split);

      IntConsumer consumer = results::add;

      while (spliterator.tryAdvance(consumer)) { }

      assertEquals(0, results.size());
   }
}
