package org.infinispan.commons.util;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Immutable empty IntSet
 * @author wburns
 * @since 9.3
 */
class EmptyIntSet extends AbstractImmutableIntSet {
   // We just use default hashCode as we only have 1 instance
   private final static EmptyIntSet INSTANCE = new EmptyIntSet();

   public static IntSet getInstance() {
      return INSTANCE;
   }

   @Override
   public boolean contains(int i) {
      return false;
   }

   @Override
   public boolean containsAll(IntSet set) {
      return set.isEmpty();
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public boolean isEmpty() {
      return true;
   }

   @Override
   public boolean contains(Object o) {
      return false;
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return EmptyIntIterator.INSTANCE;
   }

   private static class EmptyIntIterator implements PrimitiveIterator.OfInt {
      private static final EmptyIntIterator INSTANCE = new EmptyIntIterator();

      @Override
      public int nextInt() {
         throw new NoSuchElementException();
      }

      @Override
      public boolean hasNext() {
         return false;
      }
   }

   @Override
   public Object[] toArray() {
      return new Object[0];
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return a;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return c.isEmpty();
   }

   @Override
   public IntStream intStream() {
      return IntStream.empty();
   }

   @Override
   public boolean equals(Object obj) {
      if (obj instanceof Set) {
         return ((Set) obj).size() == 0;
      }
      return false;
   }

   @Override
   public String toString() {
      return "{}";
   }
}
