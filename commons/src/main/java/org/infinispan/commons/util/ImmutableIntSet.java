package org.infinispan.commons.util;

import java.util.Collection;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * Immutable wrapper for {@link IntSet}.
 *
 * @author Dan Berindei
 * @since 9.2
 * @deprecated since 9.3 This class will no longer be public, please use {@link IntSets#immutableSet(IntSet)}
 */
@Deprecated
public class ImmutableIntSet extends AbstractImmutableIntSet {
   private final IntSet set;

   public ImmutableIntSet(IntSet set) {
      this.set = set;
   }

   @Override
   public boolean contains(int i) {
      return set.contains(i);
   }

   @Override
   public boolean addAll(IntSet set) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsAll(IntSet set) {
      return this.set.containsAll(set);
   }

   @Override
   public int size() {
      return set.size();
   }

   @Override
   public boolean isEmpty() {
      return set.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return set.contains(o);
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return new ImmutableIterator(set.iterator());
   }

   @Override
   public int[] toIntArray() {
      return set.toIntArray();
   }

   @Override
   public Object[] toArray() {
      return set.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return set.toArray(a);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return set.containsAll(c);
   }

   @Override
   public IntStream intStream() {
      return set.intStream();
   }

   @Override
   public void forEach(IntConsumer action) {
      set.forEach(action);
   }

   @Override
   public void forEach(Consumer<? super Integer> action) {
      set.forEach(action);
   }

   @Override
   public Spliterator.OfInt intSpliterator() {
      return set.intSpliterator();
   }

   private class ImmutableIterator implements PrimitiveIterator.OfInt {
      private OfInt iterator;

      ImmutableIterator(OfInt iterator) {
         this.iterator = iterator;
      }

      @Override
      public int nextInt() {
         return iterator.nextInt();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }
   }
}
