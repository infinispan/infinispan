package org.infinispan.commons.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Collections;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author wburns
 * @since 9.0
 */
@RunWith(Parameterized.class)
public class MutableIntSetTest {
   @Parameterized.Parameters
   public static Object[] data() {
      return new Object[] {
            new SmallIntSet(),
            new ConcurrentSmallIntSet(64),
      };
   }

   @Parameterized.Parameter
   public IntSet intSet;

   @After
   public void after() {
      intSet.clear();
   }

   @Test
   public void testSize() {
      intSet.add(1);
      intSet.add(4);
      assertEquals(2, intSet.size());
      intSet.add(4);
      assertEquals(2, intSet.size());
   }
   @Test
   public void testFlaky1() {
        double r = Math.random();
        if (r < 0.5) {
            fail("oops");
        }
    }

   @Test
   public void testFlaky1() {
        double r = Math.random();
        if (r < 0.3) {
            fail("oops");
        }
    }

   @Test
   public void testFlaky2() {
      double r = Math.random();
      if (r < 0.3) {
            fail("oops2");
      }
   }

   @Test
   public void testIsEmpty() {
      assertTrue(intSet.isEmpty());
      intSet.add(1);
      intSet.add(4);
      assertFalse(intSet.isEmpty());
   }

   @Test
   public void testContains() throws Exception {
      assertFalse(intSet.contains(1));
      intSet.add(1);
      assertTrue(intSet.contains(1));

      assertFalse(intSet.contains(1832131));
   }

   @Test
   public void testContains1() throws Exception {
      Integer intValue = 1;
      assertFalse(intSet.contains(intValue));
      intSet.add(1);
      assertTrue(intSet.contains(intValue));

      assertFalse(intSet.contains(1832131));
   }

   @Test
   public void testIterator() throws Exception {
      intSet.add(1);
      intSet.add(4);
      PrimitiveIterator.OfInt iterator = intSet.iterator();
      assertEquals(1, iterator.nextInt());
      assertEquals(4, iterator.nextInt());
   }

   @Test
   public void testToIntArray() throws Exception {
      intSet.add(1);
      intSet.add(4);
      int[] array = intSet.toIntArray();
      assertArrayEquals(new int[]{1, 4}, array);
   }

   @Test
   public void testToIntArray64() throws Exception {
      addRange64();

      int[] array = intSet.toIntArray();
      assertArrayEquals(new RangeSet(64).toIntArray(), array);
   }

   @Test
   public void testToArray() throws Exception {
      intSet.add(1);
      intSet.add(4);
      Object[] array = intSet.toArray();
      assertArrayEquals(new Object[]{1, 4}, array);
   }

   @Test
   public void testToArray1() throws Exception {
      intSet.add(1);
      intSet.add(4);
      Integer[] array = intSet.toArray(new Integer[2]);
      assertArrayEquals(new Integer[]{1, 4}, array);
   }

   @Test
   public void testAdd() throws Exception {
      assertTrue(intSet.add(1));
      assertFalse(intSet.add(1));
   }

   @Test
   public void testAdd1() throws Exception {
      assertTrue(intSet.add(Integer.valueOf(1)));
      assertFalse(intSet.add(Integer.valueOf(1)));
   }

   @Test
   public void testSet() throws Exception {
      intSet.set(1);
      assertTrue(intSet.contains(1));
   }

   @Test
   public void testRemove() throws Exception {
      assertFalse(intSet.remove(1));
      intSet.add(1);
      assertTrue(intSet.remove(1));
   }

   @Test
   public void testRemove1() throws Exception {
      assertFalse(intSet.remove(Integer.valueOf(1)));
      intSet.add(1);
      assertTrue(intSet.remove(Integer.valueOf(1)));
   }

   @Test
   public void testContainsAll() throws Exception {
      IntSet intSet2 = new SmallIntSet();

      intSet.add(1);
      intSet.add(4);

      intSet2.add(1);
      intSet2.add(4);
      intSet2.add(7);

      assertFalse(intSet.containsAll(intSet2));
      assertTrue(intSet2.containsAll(intSet));
   }

   @Test
   public void testContainsAll1() throws Exception {
      intSet.add(1);
      intSet.add(4);

      Set<Integer> set = Util.asSet(1, 4, 7);

      assertFalse(intSet.containsAll(set));
      assertTrue(set.containsAll(intSet));
   }

   @Test
   public void testAddAll() throws Exception {
      intSet.add(1);
      intSet.add(4);

      IntSet intSet2 = new SmallIntSet();
      intSet2.addAll(intSet);
      assertEquals(2, intSet2.size());
   }

   @Test
   public void testAddAll1() throws Exception {
      IntSet rangeSet = new RangeSet(4);

      intSet.addAll(rangeSet);
      assertEquals(4, intSet.size());
   }

   @Test
   public void testAddAll2() throws Exception {
      intSet.addAll(Util.asSet(1, 4));
      assertEquals(2, intSet.size());
   }

   @Test
   public void testRemoveAll() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      IntSet intSet2 = new SmallIntSet();
      intSet2.add(4);
      intSet2.add(5);
      intSet2.add(7);

      assertTrue(intSet.removeAll(intSet2));

      assertEquals(1, intSet.size());
      assertTrue(intSet.contains(1));
   }

   @Test
   public void testRemoveAll1() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      // (1 - 4)
      IntSet rs = new RangeSet(5);

      assertTrue(intSet.removeAll(rs));

      assertEquals(Util.asSet((Object) 7), intSet);
   }

   @Test
   public void testRemoveAll2() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      assertTrue(intSet.removeAll(Util.asSet(4, 5, 7)));

      assertEquals(Util.asSet((Object) 1), intSet);
   }

   @Test
   public void testRetainAll() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      IntSet intSet2 = new SmallIntSet();
      intSet2.add(4);
      intSet2.add(5);
      intSet2.add(7);

      intSet.retainAll(intSet2);

      assertEquals(Util.asSet(4, 7), intSet);
   }

   @Test
   public void testRetainAll1() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      // (0 - 4)
      IntSet rs = new RangeSet(5);

      assertTrue(intSet.retainAll(rs));

      assertEquals(Util.asSet(1, 4), intSet);
   }

   @Test
   public void testRetainAll2() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      assertTrue(intSet.retainAll(Util.asSet(4, 5, 7)));

      assertEquals(Util.asSet(4, 7), intSet);
   }

   @Test
   public void testClear() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(3, intSet.size());

      intSet.clear();

      assertEquals(0, intSet.size());
   }

   @Test
   public void testClear64() {
      addRange64();

      assertEquals(64, intSet.size());

      intSet.clear();

      assertEquals(0, intSet.size());
   }

   public void addRange64() {
      for (int i = 0; i < 64; i++) {
         intSet.add(i);
      }
   }

   @Test
   public void testIntStream() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertEquals(12, intSet.intStream().sum());
   }

   @Test
   public void testEquals() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      IntSet intSet2 = new SmallIntSet();
      intSet2.add(1);
      intSet2.add(4);

      // Verify equals both ways
      assertNotEquals(intSet, intSet2);
      assertNotEquals(intSet2, intSet);

      intSet2.add(7);

      assertEquals(intSet, intSet2);
      assertEquals(intSet2, intSet);
   }

   @Test
   public void testEquals1() throws Exception {
      intSet.add(0);
      intSet.add(1);
      intSet.add(2);

      // (0 - 3)
      IntSet rangeSet = new RangeSet(4);

      // Verify equals both ways
      assertNotEquals(rangeSet, intSet);
      assertNotEquals(intSet, rangeSet);

      intSet.add(3);

      assertEquals(intSet, intSet);
      assertEquals(intSet, intSet);
   }

   @Test
   public void testEquals2() throws Exception {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      Set<Integer> hashSet = Util.asSet(1, 4);

      // Verify equals both ways
      assertNotEquals(intSet, hashSet);
      assertNotEquals(hashSet, intSet);

      hashSet.add(7);

      assertEquals(intSet, hashSet);
      assertEquals(hashSet, intSet);
   }

   @Test
   public void testForEachPrimitive() {
      intSet.add(0);
      intSet.add(4);
      intSet.add(7);

      Set<Integer> results = new HashSet<>();

      intSet.forEach((IntConsumer) results::add);

      assertEquals(Util.asSet(0, 4, 7), results);
   }

   @Test
   public void testForEachPrimitive64() {
      addRange64();

      Set<Integer> results = new HashSet<>();

      intSet.forEach((IntConsumer) results::add);

      assertEquals(new RangeSet(64), results);
   }

   @Test
   public void testForEachObject() {
      intSet.add(0);
      intSet.add(4);
      intSet.add(7);

      Set<Integer> results = new HashSet<>();

      intSet.forEach((Consumer<? super Integer>) results::add);

      assertEquals(Util.asSet(0, 4, 7), results);
   }

   @Test
   public void testRemoveIfPrimitive() {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertFalse(intSet.removeIf((int i) -> i > 10));
      assertEquals(3, intSet.size());
      assertTrue(intSet.removeIf((int i) -> true));
      assertEquals(0, intSet.size());
      assertEquals(Collections.emptySet(), intSet);
   }

   @Test
   public void testRemoveIf64() {
      addRange64();

      assertFalse(intSet.removeIf((int i) -> i > 64));
      assertEquals(64, intSet.size());
      assertTrue(intSet.removeIf((int i) -> true));
      assertEquals(0, intSet.size());
      assertEquals(Collections.emptySet(), intSet);
   }

   @Test
   public void testRemoveIfObject() {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      assertFalse(intSet.removeIf((Integer i) -> i / 10 > 0));
      assertEquals(3, intSet.size());
      assertTrue(intSet.removeIf((Integer i) -> i > 3));
      assertEquals(Util.asSet((Object) 1), intSet);
   }

   @Test
   public void testIntSpliteratorForEachRemaining() {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      Set<Integer> results = new HashSet<>();

      intSet.intSpliterator().forEachRemaining((IntConsumer) results::add);

      assertEquals(Util.asSet(1, 4, 7), results);
   }

   @Test
   public void testIntSpliteratorSplitTryAdvance() {
      intSet.add(1);
      intSet.add(4);
      intSet.add(7);

      Set<Integer> results = new HashSet<>();

      Spliterator.OfInt spliterator = intSet.intSpliterator();
      Spliterator.OfInt split = spliterator.trySplit();

      assertNotNull(split);

      IntConsumer consumer = results::add;

      while (spliterator.tryAdvance(consumer)) { }

      while (split.tryAdvance(consumer)) { }

      assertEquals(Util.asSet(1, 4, 7), results);
   }

   @Test
   public void testToByteSet() {
      intSet.add(3);

      intSet.add(16);

      intSet.add(35);
      intSet.add(34);
      intSet.add(33);

      byte[] bytes = intSet.toBitSet();
      byte[] expectedBytes = new byte[] { 8, 0, 1, 0, 14};
      if (bytes.length == expectedBytes.length) {
         assertArrayEquals(expectedBytes, bytes);
      } else {
         for (int i = 0; i < expectedBytes.length; ++i) {
            assertEquals("Byte at pos: " + i + " didn't match", expectedBytes[i], bytes[i]);
         }
         // Any extra bytes should be all 0
         for (int i = expectedBytes.length; i < bytes.length; ++i) {
            assertEquals("Byte at pos: " + i + " didn't match", 0, bytes[i]);
         }
      }
   }
}
