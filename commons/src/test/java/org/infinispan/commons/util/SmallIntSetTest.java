package org.infinispan.commons.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * @author wburns
 * @since 9.0
 */
public class SmallIntSetTest {
   @Test
   public void testSize() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      assertEquals(2, sis.size());
      sis.add(4);
      assertEquals(2, sis.size());
   }

   @Test
   public void testIsEmpty() {
      SmallIntSet sis = new SmallIntSet();
      assertTrue(sis.isEmpty());
      sis.add(1);
      sis.add(4);
      assertFalse(sis.isEmpty());
   }

   @Test
   public void contains() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      assertFalse(sis.contains(1));
      sis.add(1);
      assertTrue(sis.contains(1));
   }

   @Test
   public void contains1() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      Integer intValue = 1;
      assertFalse(sis.contains(intValue));
      sis.add(1);
      assertTrue(sis.contains(intValue));
   }

   @Test
   public void iterator() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      PrimitiveIterator.OfInt iterator = sis.iterator();
      assertEquals(1, iterator.nextInt());
      assertEquals(4, iterator.nextInt());
   }

   @Test
   public void toArray() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      Object[] array = sis.toArray();
      assertEquals(2, array.length);
      assertEquals(1, array[0]);
      assertEquals(4, array[1]);
   }

   @Test
   public void toArray1() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      Object[] array = sis.toArray(new Integer[2]);
      assertEquals(2, array.length);
      assertEquals(1, array[0]);
      assertEquals(4, array[1]);
   }

   @Test
   public void add() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      assertTrue(sis.add(1));
      assertFalse(sis.add(1));
   }

   @Test
   public void add1() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      assertTrue(sis.add(Integer.valueOf(1)));
      assertFalse(sis.add(Integer.valueOf(1)));
   }

   @Test
   public void set() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.set(1);
      assertTrue(sis.contains(1));
   }

   @Test
   public void remove() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      assertFalse(sis.remove(1));
      sis.add(1);
      assertTrue(sis.remove(1));
   }

   @Test
   public void remove1() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      assertFalse(sis.remove(Integer.valueOf(1)));
      sis.add(1);
      assertTrue(sis.remove(Integer.valueOf(1)));
   }

   @Test
   public void containsAll() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      SmallIntSet sis2 = new SmallIntSet();

      sis1.add(1);
      sis1.add(4);

      sis2.add(1);
      sis2.add(4);
      sis2.add(7);

      assertFalse(sis1.containsAll(sis2));
      assertTrue(sis2.containsAll(sis1));
   }

   @Test
   public void containsAll1() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      Set<Integer> sis2 = new HashSet<>();

      sis1.add(1);
      sis1.add(4);

      sis2.add(1);
      sis2.add(4);
      sis2.add(7);

      assertFalse(sis1.containsAll(sis2));
      assertTrue(sis2.containsAll(sis1));
   }

   @Test
   public void addAll() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);

      SmallIntSet sis2 = new SmallIntSet();
      sis2.addAll(sis1);
      assertEquals(2, sis2.size());
   }

   @Test
   public void addAll1() throws Exception {
      IntSet intSet = new RangeSet(4);

      SmallIntSet sis = new SmallIntSet();
      sis.addAll(intSet);
      assertEquals(4, sis.size());
   }

   @Test
   public void addAll2() throws Exception {
      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(1);
      hashSet.add(4);

      SmallIntSet sis = new SmallIntSet();
      sis.addAll(hashSet);
      assertEquals(2, sis.size());
   }

   @Test
   public void removeAll() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      assertEquals(3, sis1.size());

      SmallIntSet sis2 = new SmallIntSet();
      sis2.add(4);
      sis2.add(5);
      sis2.add(7);

      sis1.removeAll(sis2);

      assertEquals(1, sis1.size());
      assertTrue(sis1.contains(1));
   }

   @Test
   public void removeAll1() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      assertEquals(3, sis1.size());

      // (1 - 4)
      IntSet intSet = new RangeSet(5);

      sis1.removeAll(intSet);

      assertEquals(1, sis1.size());
      assertTrue(sis1.contains(7));
   }

   @Test
   public void removeAll2() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      assertEquals(3, sis1.size());

      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(4);
      hashSet.add(5);
      hashSet.add(7);

      sis1.removeAll(hashSet);

      assertEquals(1, sis1.size());
      assertTrue(sis1.contains(1));
   }

   @Test
   public void retainAll() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      assertEquals(3, sis1.size());

      SmallIntSet sis2 = new SmallIntSet();
      sis2.add(4);
      sis2.add(5);
      sis2.add(7);

      sis1.retainAll(sis2);

      assertEquals(2, sis1.size());
      assertTrue(sis1.contains(4));
      assertTrue(sis1.contains(7));
   }

   @Test
   public void retainAll1() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      assertEquals(3, sis1.size());

      // (1 - 4)
      IntSet intSet = new RangeSet(5);

      sis1.retainAll(intSet);

      assertEquals(2, sis1.size());
      assertTrue(sis1.contains(1));
      assertTrue(sis1.contains(4));
   }

   @Test
   public void retainAll2() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      assertEquals(3, sis1.size());

      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(4);
      hashSet.add(5);
      hashSet.add(7);

      sis1.retainAll(hashSet);

      assertEquals(2, sis1.size());
      assertTrue(sis1.contains(4));
      assertTrue(sis1.contains(7));
   }

   @Test
   public void clear() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      assertEquals(3, sis.size());

      sis.clear();

      assertEquals(0, sis.size());
   }

   @Test
   public void intStream() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      assertEquals(12, sis.intStream().sum());
   }

   @Test
   public void equals() throws Exception {
      SmallIntSet sis1 = new SmallIntSet();
      sis1.add(1);
      sis1.add(4);
      sis1.add(7);

      SmallIntSet sis2 = new SmallIntSet();
      sis2.add(1);
      sis2.add(4);

      // Verify equals both ways
      assertNotEquals(sis1, sis2);
      assertNotEquals(sis2, sis1);

      sis2.add(7);

      assertEquals(sis1, sis2);
      assertEquals(sis2, sis1);
   }

   @Test
   public void equals1() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(0);
      sis.add(1);
      sis.add(2);

      // (0 - 3)
      IntSet intSet = new RangeSet(4);

      // Verify equals both ways
      assertNotEquals(sis, intSet);
      assertNotEquals(intSet, sis);

      sis.add(3);

      assertEquals(sis, intSet);
      assertEquals(intSet, sis);
   }

   @Test
   public void equals2() throws Exception {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(1);
      hashSet.add(4);

      // Verify equals both ways
      assertNotEquals(sis, hashSet);
      assertNotEquals(hashSet, sis);

      hashSet.add(7);

      assertEquals(sis, hashSet);
      assertEquals(hashSet, sis);
   }

   @Test
   public void forEachPrimitive() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      Set<Integer> results = new HashSet<>();

      sis.forEach((IntConsumer) results::add);

      assertEquals(3, results.size());
      assertTrue(results.contains(1));
      assertTrue(results.contains(4));
      assertTrue(results.contains(7));
   }

   @Test
   public void forEachObject() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      Set<Integer> results = new HashSet<>();

      sis.forEach((Consumer<? super Integer>) results::add);

      assertEquals(3, results.size());
      assertTrue(results.contains(1));
      assertTrue(results.contains(4));
      assertTrue(results.contains(7));
   }

   @Test
   public void removeIfPrimitive() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      assertFalse(sis.removeIf((int i) -> i / 10 > 0));
      assertEquals(3, sis.size());
      assertTrue(sis.removeIf((int i) -> i > 3));
      assertEquals(1, sis.size());
      assertTrue(sis.contains(1));
   }

   @Test
   public void removeIfObject() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      assertFalse(sis.removeIf((Integer i) -> i / 10 > 0));
      assertEquals(3, sis.size());
      assertTrue(sis.removeIf((Integer i) -> i > 3));
      assertEquals(1, sis.size());
      assertTrue(sis.contains(1));
   }

   @Test
   public void intSpliteratorForEachRemaining() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      Set<Integer> results = new HashSet<>();

      sis.intSpliterator().forEachRemaining((IntConsumer) results::add);

      assertEquals(3, results.size());
      assertTrue(results.contains(1));
      assertTrue(results.contains(4));
      assertTrue(results.contains(7));
   }

   @Test
   public void intSpliteratorSplitTryAdvance() {
      SmallIntSet sis = new SmallIntSet();
      sis.add(1);
      sis.add(4);
      sis.add(7);

      Set<Integer> results = new HashSet<>();

      Spliterator.OfInt spliterator = sis.intSpliterator();
      Spliterator.OfInt split = spliterator.trySplit();

      assertNotNull(split);

      IntConsumer consumer = results::add;

      while (spliterator.tryAdvance(consumer)) { }

      while (split.tryAdvance(consumer)) { }

      assertEquals(3, results.size());
      assertTrue(results.contains(1));
      assertTrue(results.contains(4));
      assertTrue(results.contains(7));
   }
}
