package org.infinispan.commons.util;

import java.util.Collection;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;

/**
 * Immutable wrapper for {@link IntSet}.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class ImmutableIntSet implements IntSet {
   private final IntSet set;

   public ImmutableIntSet(IntSet set) {
      this.set = set;
   }

   @Override
   public boolean add(int i) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void set(int i) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(int i) {
      throw new UnsupportedOperationException();
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
   public boolean removeAll(IntSet set) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(IntSet c) {
      throw new UnsupportedOperationException();
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
   public Object[] toArray() {
      return set.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return set.toArray(a);
   }

   @Override
   public boolean add(Integer integer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return set.containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
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

   @Override
   public boolean removeIf(IntPredicate filter) {
      throw new UnsupportedOperationException();
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
