package org.infinispan.context;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

/**
 * @since 14.0
 */
@Test(groups = "unit", testName = "context.FlagBitsetTest")
public class FlagBitsetTest extends AbstractInfinispanTest {

   private static final Flag[] FLAGS_CACHED = Flag.values();

   public void testUniqueness() {

      Map<Long, Flag> bits = new HashMap<>(FLAGS_CACHED.length);

      for (Flag flag : FLAGS_CACHED) {
         Flag existing = bits.putIfAbsent(EnumUtil.bitSetOf(flag), flag);
         assertNull("Conflict flags: " + existing + " and " + flag, existing);
      }
   }

   public void testBitSetOf() {
      int startIdx = ThreadLocalRandom.current().nextInt(FLAGS_CACHED.length - 4);
      Flag f1 = FLAGS_CACHED[startIdx];
      Flag f2 = FLAGS_CACHED[startIdx + 1];
      Flag f3 = FLAGS_CACHED[startIdx + 2];
      Flag f4 = FLAGS_CACHED[startIdx + 3];

      log.debugf("Flags: %s, %s, %s, %s", f1, f2, f3, f4);

      assertBitSet(EnumUtil.bitSetOf(f1), startIdx, 1);
      assertBitSet(EnumUtil.bitSetOf(f1, f2), startIdx, 2);
      assertBitSet(EnumUtil.bitSetOf(f1, f2, f3), startIdx, 3);
      assertBitSet(EnumUtil.bitSetOf(f1, f2, f3, f4), startIdx, 4);
   }

   public void testEnumFromBitSet() {
      int startIdx = ThreadLocalRandom.current().nextInt(FLAGS_CACHED.length - 4);
      Flag f1 = FLAGS_CACHED[startIdx];
      Flag f2 = FLAGS_CACHED[startIdx + 1];
      Flag f3 = FLAGS_CACHED[startIdx + 2];
      Flag f4 = FLAGS_CACHED[startIdx + 3];

      log.debugf("Flags: %s, %s, %s, %s", f1, f2, f3, f4);

      assertEquals(EnumSet.of(f1), EnumUtil.enumSetOf(EnumUtil.bitSetOf(f1), Flag.class));
      assertEquals(EnumSet.of(f1, f2), EnumUtil.enumSetOf(EnumUtil.bitSetOf(f1, f2), Flag.class));
      assertEquals(EnumSet.of(f1, f2, f3), EnumUtil.enumSetOf(EnumUtil.bitSetOf(f1, f2, f3), Flag.class));
      assertEquals(EnumSet.of(f1, f2, f3, f4), EnumUtil.enumSetOf(EnumUtil.bitSetOf(f1, f2, f3, f4), Flag.class));
   }

   public void testEnumSet() {
      int startIdx = ThreadLocalRandom.current().nextInt(FLAGS_CACHED.length - 4);
      Flag f1 = FLAGS_CACHED[startIdx];
      Flag f2 = FLAGS_CACHED[startIdx + 1];
      Flag f3 = FLAGS_CACHED[startIdx + 2];
      Flag f4 = FLAGS_CACHED[startIdx + 3];

      log.debugf("Flags: %s, %s, %s, %s", f1, f2, f3, f4);

      assertBitSet(EnumUtil.setEnum(EnumUtil.bitSetOf(f1), f2), startIdx, 2);
      assertBitSet(EnumUtil.setEnums(EnumUtil.bitSetOf(f1), Arrays.asList(f2, f3, f4)), startIdx, 4);
   }

   public void testEnumUnset() {
      int startIdx = ThreadLocalRandom.current().nextInt(FLAGS_CACHED.length - 4);
      Flag f1 = FLAGS_CACHED[startIdx];
      Flag f2 = FLAGS_CACHED[startIdx + 1];
      Flag f3 = FLAGS_CACHED[startIdx + 2];

      log.debugf("Flags: %s, %s, %s", f1, f2, f3);

      assertBitSet(EnumUtil.unsetEnum(EnumUtil.bitSetOf(f1, f2, f3), f3), startIdx, 2);
   }

   public void testBitSetOperations() {
      int startIdx = ThreadLocalRandom.current().nextInt(FLAGS_CACHED.length - 4);
      Flag f1 = FLAGS_CACHED[startIdx];
      Flag f2 = FLAGS_CACHED[startIdx + 1];
      Flag f3 = FLAGS_CACHED[startIdx + 2];
      Flag f4 = FLAGS_CACHED[startIdx + 3];

      log.debugf("Flags: %s, %s, %s, %s", f1, f2, f3, f4);

      assertBitSet(EnumUtil.mergeBitSets(EnumUtil.bitSetOf(f1), EnumUtil.bitSetOf(f2, f3)), startIdx, 3);
      assertBitSet(EnumUtil.diffBitSets(EnumUtil.bitSetOf(f1, f2, f3, f4), EnumUtil.bitSetOf(f4)), startIdx, 3);

      assertTrue(EnumUtil.containsAll(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f1, f2)));
      assertTrue(EnumUtil.containsAll(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f1)));
      assertFalse(EnumUtil.containsAll(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f1, f3)));
      assertFalse(EnumUtil.containsAll(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f4)));

      assertTrue(EnumUtil.containsAny(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f1, f2)));
      assertTrue(EnumUtil.containsAny(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f1)));
      assertTrue(EnumUtil.containsAny(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f1, f3)));
      assertFalse(EnumUtil.containsAny(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f3, f4)));
      assertFalse(EnumUtil.containsAny(EnumUtil.bitSetOf(f1, f2), EnumUtil.bitSetOf(f3)));
   }

   private static void assertBitSet(long bitSet, int startIdx, int range) {
      IntStream.range(0, startIdx).forEach(idx -> assertNotFlag(bitSet, FLAGS_CACHED[idx]));
      IntStream.range(startIdx, startIdx + range).forEach(idx -> assertFlag(bitSet, FLAGS_CACHED[idx]));
      IntStream.range(startIdx + range, FLAGS_CACHED.length).forEach(idx -> assertNotFlag(bitSet, FLAGS_CACHED[idx]));
   }

   private static void assertFlag(long bitset, Flag flag) {
      assertTrue("Flag " + flag + " should be in bitset!", EnumUtil.hasEnum(bitset, flag));
   }

   private static void assertNotFlag(long bitset, Flag flag) {
      assertFalse("Flag " + flag + " should not be in bitset!", EnumUtil.hasEnum(bitset, flag));
   }

}
