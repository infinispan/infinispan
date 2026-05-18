package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class ArrayRingBufferTest {

   @Test
   public void testShouldEnlargeItWithGaps() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      assertEquals(1, rb.size());
      assertEquals(rb.get(1), Integer.valueOf(1));
      rb.set(3, 2);
      assertEquals(3, rb.size());
      assertEquals(rb.get(1), Integer.valueOf(1));
      assertNull(rb.get(2));
      assertEquals(rb.get(3), Integer.valueOf(2));
   }

   @Test
   public void testShouldEnlargeItWithTwiceWrap() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(6);
      for (int i = 6; i < 10; i++) {
         rb.set(i, i);
      }
      assertEquals(4, rb.availableCapacityWithoutResizing());
      rb.set(10, 10);
      assertEquals(3, rb.availableCapacityWithoutResizing());
      for (int i = 6; i < 11; i++) {
         assertEquals(rb.get(i), Integer.valueOf(i));
      }
   }

   @Test
   public void testForEach() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(6);
      for (int i = 6; i <= 10; i++) {
         rb.set(i, i);
      }
      Map<Integer, Long> expected_values = new HashMap<>(5);
      rb.forEach(expected_values::put);
      assertEquals(5, expected_values.size());
      for (Map.Entry<Integer, Long> e : expected_values.entrySet()) {
         Integer key = e.getKey();
         Long val = e.getValue();
         assertEquals(key.intValue(), val.intValue());
      }
   }

   @Test
   public void testRemove() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      for (int i = 1; i <= 10; i++)
         rb.set(i, i);
      assertEquals(10, rb.size());
      assertEquals(10, rb.size(false));
      Integer ret = rb.remove(0);
      assertNull(ret);

      ret = rb.remove(1); // head
      assertNotNull(ret);
      assertEquals(1, ret.intValue());
      assertEquals(9, rb.size());
      assertEquals(9, rb.size(false));

      ret = rb.remove(5);
      assertNotNull(ret);
      assertEquals(5, ret.intValue());
      assertEquals(8, rb.size(false), String.format("size should be 8 but is %d", rb.size(false)));

      rb.set(5, 5);
      assertEquals(9, rb.size(false));
   }

   @Test
   public void testShouldUseAvailableCapacity() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(7, 2);
      assertEquals(7, rb.size());
      rb.set(8, 3);
      assertEquals(rb.get(1), Integer.valueOf(1));
      assertEquals(rb.get(7), Integer.valueOf(2));
      assertEquals(rb.get(8), Integer.valueOf(3));
   }

   @Test
   public void testShouldCopyOldElementsInTheRightOrder() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(7, 2);
      rb.set(8, 3);
      rb.set(15, 4);
      assertEquals(rb.get(1), Integer.valueOf(1));
      assertEquals(rb.get(7), Integer.valueOf(2));
      assertEquals(rb.get(8), Integer.valueOf(3));
      assertEquals(rb.get(15), Integer.valueOf(4));
   }

   @Test
   public void testShouldClearAndIncreaseAvailableSpace() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(7, 2);
      rb.set(8, 3);
      final int before = rb.availableCapacityWithoutResizing();
      rb.dropHeadUntil(5);
      final int after = rb.availableCapacityWithoutResizing();
      assertEquals(6, after - before);
      assertEquals(rb.get(7), Integer.valueOf(2));
      assertEquals(rb.get(8), Integer.valueOf(3));
      rb.set(12, 4);
      assertEquals(2, rb.availableCapacityWithoutResizing());
      assertEquals(rb.get(12), Integer.valueOf(4));
   }

   @Test
   public void testShouldEnlargeCapacityByPowerOfTwo() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(4, 2);
      rb.set(7, 3);
      rb.set(8, 4);
      rb.dropHeadUntil(5);
      rb.set(14, 5);
      assertEquals(0, rb.availableCapacityWithoutResizing());
   }

   @Test
   public void testShouldClearUptoAlthoughWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.dropHeadUntil(11);
      assertEquals(1, rb.size());
      assertEquals(rb.get(11), Integer.valueOf(11));
   }

   @Test
   public void testShouldClearFromAlthoughWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.dropTailTo(7);
      assertEquals(3, rb.size());
      assertEquals(3, rb.size(false));
      assertEquals(rb.get(4), Integer.valueOf(4));
      assertEquals(rb.get(5), Integer.valueOf(5));
      assertEquals(rb.get(6), Integer.valueOf(6));
   }

   @Test
   public void testShouldClearUpToAllWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.clear();
      assertEquals(0, rb.size());
   }

   @Test
   public void testShouldClearFromAllWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.dropTailTo(rb.getHeadSequence());
      assertEquals(0, rb.size());
   }

   @Test
   public void testDropTailTo() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      for (int i = 1; i <= 20; i++)
         rb.set(i, i);
      assertEquals(20, rb.size());
      for (int i = 1; i <= 20; i++)
         assertEquals((int) rb.get(i), i);
      rb.dropTailTo(5);
      assertEquals(4, rb.size());
      try {
         assertEquals(0, (int) rb.get(5)); // drop was *inclusive*
         fail("index 5 should not be found");
      } catch (IllegalArgumentException t) {
         System.out.println("get(5) triggered an exception, as expected");
      }
      for (int i = 1; i <= 4; i++)
         assertEquals((int) rb.get(i), i);
   }

   @Test
   public void testDropHeadUntil() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      for (int i = 1; i <= 20; i++)
         rb.set(i, i);
      assertEquals(20, rb.size());
      rb.dropHeadUntil(6);
      assertEquals(15, rb.size());
      try {
         rb.get(5);
         fail("index 5 should not be found");
      } catch (IllegalArgumentException ex) {
         System.out.println("get(5) triggered an exception, as expected");
      }
      for (int i = 6; i <= 20; i++) {
         assertEquals((int) rb.get(i), i);
      }
   }

   @Test
   public void testCannotAccessClearedUpData() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(5, 3);
      rb.set(15, 4);
      rb.dropHeadUntil(5);
      assertThrows(IllegalArgumentException.class, () -> rb.get(4));
   }

   @Test
   public void testCreatingBackedArrayOfSpecificSize() {
      assertEquals(8, new ArrayRingBuffer<Integer>(8, 0).availableCapacityWithoutResizing());
   }

   @Test
   public void testAddPeekPollIsEmptySizeConsistency() {
      final int initialHead = 10;
      final int size = 10;
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(initialHead, initialHead);
      for (int i = 0; i < size; i++) {
         rb.add(i);
      }
      assertEquals(size, rb.size());
      for (int i = 0; i < 10; i++) {
         final Integer expected = i;
         assertEquals(rb.peek(), expected);
         assertEquals(rb.poll(), expected);
         assertEquals(rb.size(), size - (i + 1));
         assertEquals(rb.getHeadSequence(), initialHead + i + 1);
      }
      assertTrue(rb.isEmpty());
      assertNull(rb.peek());
      assertNull(rb.poll());
      assertEquals(0, rb.size());
   }

}
