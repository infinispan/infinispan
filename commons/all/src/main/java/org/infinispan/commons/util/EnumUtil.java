package org.infinispan.commons.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.EnumSet;

/**
 * Utilities method to Enums.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class EnumUtil {

   private EnumUtil() {
   }

   public static final long EMPTY_BIT_SET = 0L;

   public static <E extends Enum<E>> long bitSetOf(Collection<E> enums) {
      if (enums == null || enums.isEmpty()) {
         return EMPTY_BIT_SET;
      }
      long flagBitSet = EMPTY_BIT_SET;
      for (Enum<?> f : enums) {
         flagBitSet |= bitSetOf(f);
      }
      return flagBitSet;
   }

   public static long bitSetOf(Enum<?> first) {
      return 1L << first.ordinal();
   }

   public static long bitSetOf(Enum<?> first, Enum<?> second) {
      return bitSetOf(first) | bitSetOf(second);
   }

   public static long bitSetOf(Enum<?> first, Enum<?> second, Enum<?>... remaining) {
      long bitSet = bitSetOf(first, second);
      for (Enum<?> f : remaining) {
         bitSet |= bitSetOf(f);
      }
      return bitSet;
   }

   public static long bitSetOf(Enum<?>[] flags) {
      long bitSet = EMPTY_BIT_SET;
      for (Enum<?> flag : flags) {
         bitSet |= bitSetOf(flag);
      }
      return bitSet;
   }

   public static <E extends Enum<E>> EnumSet<E> enumSetOf(long bitSet, Class<E> eClass) {
      if (bitSet == EMPTY_BIT_SET) {
         return EnumSet.noneOf(eClass);
      }
      EnumSet<E> flagSet = EnumSet.noneOf(eClass);
      for (E f : eClass.getEnumConstants()) {
         if (hasEnum(bitSet, f)) {
            flagSet.add(f);
         }
      }
      return flagSet;
   }

   public static boolean hasEnum(long bitSet, Enum<?> anEnum) {
      return (bitSet & bitSetOf(anEnum)) != 0;
   }

   public static long setEnum(long bitSet, Enum<?> anEnum) {
      return bitSet | bitSetOf(anEnum);
   }

   public static <E extends Enum<E>> long setEnums(long bitSet, Collection<E> enums) {
      if (enums == null || enums.isEmpty()) {
         return bitSet;
      }
      for (Enum<?> f : enums) {
         bitSet |= bitSetOf(f);
      }
      return bitSet;
   }

   public static long unsetEnum(long bitSet, Enum<?> anEnum) {
      return bitSet & ~bitSetOf(anEnum);
   }

   public static <E extends Enum<E>> String prettyPrintBitSet(long bitSet, Class<E> eClass) {
      return enumSetOf(bitSet, eClass).toString();
   }

   public static long mergeBitSets(long bitSet1, long bitSet2) {
      return bitSet1 | bitSet2;
   }

   public static long diffBitSets(long bitSet1, long bitSet2) {
      return bitSet1 & ~bitSet2;
   }

   public static boolean containsAll(long bitSet, long testBitSet) {
      return (bitSet & testBitSet) == testBitSet;
   }

   public static boolean containsAny(long bitSet, long testBitSet) {
      return (bitSet & testBitSet) != 0;
   }

   public static int bitSetSize(long bitSet) {
      return Long.bitCount(bitSet);
   }

   public static <E extends Enum<E>> E[] enumArrayOf(long bitSet, Class<E> eClass) {
      if (bitSet == EMPTY_BIT_SET) {
         return null;
      }

      E[] array = (E[]) Array.newInstance(eClass, bitSetSize(bitSet));
      int i = 0;
      for (E f : eClass.getEnumConstants()) {
         if (hasEnum(bitSet, f)) {
            array[i++] = f;
         }
      }
      return array;
   }
}
