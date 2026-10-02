package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CountingBloomFilter}, most importantly that removing values never introduces a false negative.
 *
 * @since 16.3
 */
public class CountingBloomFilterTest {

   /**
    * A filter where the position of a value is the value itself, so that the tests can reason about which values
    * share a counter.
    */
   private static CountingBloomFilter<Integer> identityFilter(int counterCount) {
      return CountingBloomFilter.createFilter(counterCount,
            List.<ToIntFunction<? super Integer>>of(Integer::intValue));
   }

   private static byte[] value(int i) {
      return ("key-" + i).getBytes(StandardCharsets.UTF_8);
   }

   @Test
   public void testNonPositiveCounterCountIsRejected() {
      assertThrows(IllegalArgumentException.class, () -> identityFilter(0));
      assertThrows(IllegalArgumentException.class, () -> identityFilter(-1));
   }

   @Test
   public void testAddAndRemoveSingleValue() {
      CountingBloomFilter<Integer> filter = identityFilter(8);

      assertFalse(filter.possiblyPresent(1));
      assertTrue(filter.addToFilter(1));
      assertTrue(filter.possiblyPresent(1));
      assertEquals(1, filter.occupiedPositions());

      assertTrue(filter.removeFromFilter(1));
      assertFalse(filter.possiblyPresent(1));
      assertEquals(0, filter.occupiedPositions());
   }

   @Test
   public void testValueHasToBeRemovedAsOftenAsItWasAdded() {
      CountingBloomFilter<Integer> filter = identityFilter(8);

      assertTrue(filter.addToFilter(1));
      // The second add cannot tell us anything new, the value was already there
      assertFalse(filter.addToFilter(1));

      assertTrue(filter.removeFromFilter(1));
      assertTrue(filter.possiblyPresent(1));
      assertTrue(filter.removeFromFilter(1));
      assertFalse(filter.possiblyPresent(1));
   }

   @Test
   public void testRemovingAnAbsentValueIsIgnored() {
      CountingBloomFilter<Integer> filter = identityFilter(8);
      filter.addToFilter(1);

      assertFalse(filter.removeFromFilter(2));
      assertTrue(filter.possiblyPresent(1));
      assertEquals(1, filter.occupiedPositions());
   }

   @Test
   public void testValuesSharingAPositionDoNotRemoveEachOther() {
      CountingBloomFilter<Integer> filter = identityFilter(8);
      // Both values land on position 1
      filter.addToFilter(1);
      filter.addToFilter(9);

      assertTrue(filter.removeFromFilter(1));
      assertTrue(filter.possiblyPresent(9));

      assertTrue(filter.removeFromFilter(9));
      assertFalse(filter.possiblyPresent(9));
      assertEquals(0, filter.occupiedPositions());
   }

   @Test
   public void testCountersArePackedWithoutInterfering() {
      // Counters are packed several to an int, so also try sizes that do not fill the last one
      for (int counterCount : new int[]{1, 3, 5, 8, 13}) {
         CountingBloomFilter<Integer> filter = identityFilter(counterCount);
         for (int position = 0; position < counterCount; ++position) {
            for (int i = 0; i <= position; ++i) {
               filter.addToFilter(position);
            }
         }
         assertEquals(counterCount, filter.occupiedPositions());

         for (int position = 0; position < counterCount; ++position) {
            for (int i = 0; i <= position; ++i) {
               assertTrue(filter.possiblyPresent(position),
                     "Position " + position + " of " + counterCount + " was lost after " + i + " removals");
               filter.removeFromFilter(position);
            }
            assertFalse(filter.possiblyPresent(position));
         }
         assertEquals(0, filter.occupiedPositions());
      }
   }

   @Test
   public void testSaturatedCounterIsNeverLowered() {
      CountingBloomFilter<Integer> filter = identityFilter(8);
      for (int i = 0; i <= CountingBloomFilter.MAX_COUNT; ++i) {
         filter.addToFilter(1);
      }
      for (int i = 0; i <= CountingBloomFilter.MAX_COUNT; ++i) {
         filter.removeFromFilter(1);
      }
      // Once a counter saturates its real value is unknown, so it may only cause extra false positives
      assertTrue(filter.possiblyPresent(1));
   }

   @Test
   public void testClearEmptiesTheFilter() {
      CountingBloomFilter<Integer> filter = identityFilter(8);
      filter.addToFilter(1);
      filter.addToFilter(1);
      filter.addToFilter(2);

      filter.clear();

      assertEquals(0, filter.occupiedPositions());
      assertFalse(filter.possiblyPresent(1));
      assertFalse(filter.possiblyPresent(2));
   }

   @Test
   public void testSetBitsReplacesTheContents() {
      CountingBloomFilter<Integer> filter = identityFilter(8);
      filter.addToFilter(1);

      IntSet positions = IntSets.mutableEmptySet(16);
      positions.set(2);
      positions.set(3);
      // Positions the filter does not have have to be ignored rather than fail
      positions.set(11);
      filter.setBits(positions);

      assertFalse(filter.possiblyPresent(1));
      assertTrue(filter.possiblyPresent(2));
      assertTrue(filter.possiblyPresent(3));

      IntSet expected = IntSets.mutableEmptySet(8);
      expected.set(2);
      expected.set(3);
      assertEquals(expected, filter.getIntSet());
   }

   @Test
   public void testRemovingValuesNeverLosesTheOnesThatAreLeft() {
      int entries = 100;
      CountingBloomFilter<byte[]> filter = MurmurHash3CountingBloomFilter.createFilter(entries * 4);

      List<byte[]> values = new ArrayList<>(entries);
      for (int i = 0; i < entries; ++i) {
         byte[] value = value(i);
         values.add(value);
         filter.addToFilter(value);
      }
      for (byte[] value : values) {
         assertTrue(filter.possiblyPresent(value));
      }

      for (int i = 0; i < entries / 2; ++i) {
         filter.removeFromFilter(values.get(i));
      }
      for (int i = entries / 2; i < entries; ++i) {
         assertTrue(filter.possiblyPresent(values.get(i)),
               "Removing other values introduced a false negative for key-" + i);
      }
   }

   @Test
   public void testMurmurHash3PositionsMatchThePlainFilter() {
      int counterCount = 64;
      BloomFilter<byte[]> plain = MurmurHash3BloomFilter.createFilter(counterCount);
      CountingBloomFilter<byte[]> counting = MurmurHash3CountingBloomFilter.createFilter(counterCount);
      for (int i = 0; i < 10; ++i) {
         plain.addToFilter(value(i));
         counting.addToFilter(value(i));
      }

      // The client computes the bits with the plain filter, so both have to agree on where a value lands
      assertArrayEquals(plain.getIntSet().toBitSet(), counting.getIntSet().toBitSet());
   }

   @Test
   public void testSetBitsFromPlainFilterHoldsEveryValue() {
      int counterCount = 64;
      BloomFilter<byte[]> plain = MurmurHash3BloomFilter.createFilter(counterCount);
      for (int i = 0; i < 10; ++i) {
         plain.addToFilter(value(i));
      }

      CountingBloomFilter<byte[]> counting = MurmurHash3CountingBloomFilter.createFilter(counterCount);
      counting.addToFilter(value(100));
      counting.setBits(plain.getIntSet());

      for (int i = 0; i < 10; ++i) {
         assertTrue(counting.possiblyPresent(value(i)));
      }
   }
}
