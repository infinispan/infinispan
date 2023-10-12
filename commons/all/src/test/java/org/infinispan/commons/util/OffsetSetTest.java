package org.infinispan.commons.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.BitSet;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.junit.Test;

public class OffsetSetTest {

   @Test
   public void testSize() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      assertEquals(4, rs.size());
   }

   @Test
   public void testIsEmpty() {
      OffsetIntSet rs = new OffsetIntSet(4, 4);
      assertTrue(rs.isEmpty());
   }

   @Test
   public void contains() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      assertFalse(rs.contains(5));
      assertTrue(rs.contains(1));
   }

   @Test
   public void contains1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      assertFalse(rs.contains(Integer.valueOf(5)));
      assertTrue(rs.contains(Integer.valueOf(1)));
   }

   @Test
   public void iterator() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      PrimitiveIterator.OfInt iterator = rs.iterator();
      assertEquals(0, iterator.nextInt());
      assertEquals(1, iterator.nextInt());
      assertEquals(2, iterator.nextInt());
      assertEquals(3, iterator.nextInt());
   }

   @Test
   public void toArray() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      Object[] array = rs.toArray();
      assertEquals(4, array.length);
      assertEquals(0, array[0]);
      assertEquals(1, array[1]);
      assertEquals(2, array[2]);
      assertEquals(3, array[3]);
   }

   @Test
   public void toArray1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      Object[] array = rs.toArray(new Integer[4]);
      assertEquals(4, array.length);
      assertEquals(0, array[0]);
      assertEquals(1, array[1]);
      assertEquals(2, array[2]);
      assertEquals(3, array[3]);
   }

   @Test
   public void toIntArray() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      int[] array = rs.toIntArray();
      assertArrayEquals(new int[]{0, 1, 2, 3}, array);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void add() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.add(1);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void add1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.add(Integer.valueOf(1));
   }

   @Test(expected = UnsupportedOperationException.class)
   public void set() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.set(1);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void remove() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.remove(1);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void remove1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.remove(Integer.valueOf(1));
   }

   @Test
   public void containsAll() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      OffsetIntSet rs2 = new OffsetIntSet(0, 4);

      assertTrue(rs.containsAll(rs2));

      OffsetIntSet rs3 = new OffsetIntSet(0, 3);

      assertTrue(rs.containsAll(rs3));
      assertFalse(rs3.containsAll(rs));
   }

   @Test
   public void containsAll1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 5);
      Set<Integer> hashSet = new HashSet<>();

      hashSet.add(1);
      hashSet.add(4);

      assertFalse(hashSet.containsAll(rs));
      assertTrue(rs.containsAll(hashSet));
   }

   @Test(expected = UnsupportedOperationException.class)
   public void addAll() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      OffsetIntSet rs2 = new OffsetIntSet(0, 5);
      rs.addAll(rs2);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void addAll1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      SmallIntSet sis = new SmallIntSet();
      rs.addAll(sis);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void addAll2() throws Exception {
      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(1);
      hashSet.add(4);

      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.addAll(hashSet);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void removeAll() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      OffsetIntSet rs2 = new OffsetIntSet(0, 6);

      rs.removeAll(rs2);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void removeAll1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 5);
      SmallIntSet sis = new SmallIntSet();

      rs.removeAll(sis);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void removeAll2() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      Set<Integer> hashSet = new HashSet<>();

      rs.removeAll(hashSet);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void retainAll() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      OffsetIntSet rs2 = new OffsetIntSet(0, 5);

      rs.retainAll(rs2);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void retainAll1() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 5);

      SmallIntSet sis = new SmallIntSet();

      rs.retainAll(sis);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void retainAll2() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);

      Set<Integer> hashSet = new HashSet<>();

      rs.retainAll(hashSet);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void clear() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 5);

      rs.clear();
   }

   @Test
   public void intStream() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 5);

      assertEquals(10, rs.intStream().sum());
   }

   @Test
   public void equals() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      OffsetIntSet rs2 = new OffsetIntSet(0, 5);

      // Verify equals both ways
      assertNotEquals(rs, rs2);
      assertNotEquals(rs2, rs);

      OffsetIntSet rs3 = new OffsetIntSet(0, 4);

      assertEquals(rs3, rs);
      assertEquals(rs, rs3);
   }

   @Test
   public void equals1() throws Exception {
      IntSet intSet = new OffsetIntSet(0, 4);

      SmallIntSet sis = new SmallIntSet();
      sis.add(0);
      sis.add(1);
      sis.add(2);

      // Verify equals both ways
      assertNotEquals(sis, intSet);
      assertNotEquals(intSet, sis);

      sis.add(3);

      assertEquals(sis, intSet);
      assertEquals(intSet, sis);
   }

   @Test
   public void equals2() throws Exception {
      OffsetIntSet rs = new OffsetIntSet(0, 4);

      Set<Integer> hashSet = new HashSet<>();
      hashSet.add(0);
      hashSet.add(1);
      hashSet.add(2);

      // Verify equals both ways
      assertNotEquals(rs, hashSet);
      assertNotEquals(hashSet, rs);

      hashSet.add(3);

      assertEquals(rs, hashSet);
      assertEquals(hashSet, rs);
   }

   @Test
   public void forEachPrimitive() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);

      Set<Integer> results = new HashSet<>();

      rs.forEach((IntConsumer) results::add);

      assertEquals(4, results.size());
      assertTrue(results.contains(0));
      assertTrue(results.contains(1));
      assertTrue(results.contains(2));
      assertTrue(results.contains(3));
   }

   @Test
   public void forEachObject() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);

      Set<Integer> results = new HashSet<>();

      rs.forEach((Consumer<? super Integer>) results::add);

      assertEquals(4, results.size());
      assertTrue(results.contains(0));
      assertTrue(results.contains(1));
      assertTrue(results.contains(2));
      assertTrue(results.contains(3));
   }

   @Test(expected = UnsupportedOperationException.class)
   public void removeIfPrimitive() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.removeIf((int i) -> i == 3);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void removeIfObject() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);
      rs.removeIf((Integer i) -> i == 3);
   }

   @Test
   public void intSpliteratorForEachRemaining() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);

      Set<Integer> results = new HashSet<>();

      rs.intSpliterator().forEachRemaining((IntConsumer) results::add);

      assertEquals(4, results.size());
      assertTrue(results.contains(0));
      assertTrue(results.contains(1));
      assertTrue(results.contains(2));
      assertTrue(results.contains(3));
   }

   @Test
   public void intSpliteratorSplitTryAdvance() {
      OffsetIntSet rs = new OffsetIntSet(0, 4);

      Set<Integer> results = new HashSet<>();

      Spliterator.OfInt spliterator = rs.intSpliterator();
      Spliterator.OfInt split = spliterator.trySplit();

      assertNotNull(split);

      IntConsumer consumer = results::add;

      while (spliterator.tryAdvance(consumer)) { }

      while (split.tryAdvance(consumer)) { }

      assertEquals(4, results.size());
      assertTrue(results.contains(0));
      assertTrue(results.contains(1));
      assertTrue(results.contains(2));
      assertTrue(results.contains(3));
   }

   @Test
   public void testToBitSet() {
      testToArray(13);
      testToArray(12);
      testToArray(0);
      testToArray(16);
      testToArray(43);
      testToArray(5);
   }

   private void testToArray(int bitToSet) {
      BitSet bitSet = new BitSet();
      for (int i = 0; i < bitToSet; ++i) {
         bitSet.set(i);
      }

      IntSet sis = new OffsetIntSet(0, bitToSet);

      assertArrayEquals(bitSet.toByteArray(), sis.toBitSet());
   }
}
