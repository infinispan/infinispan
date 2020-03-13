package org.infinispan.commons.util;

import java.util.Collection;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

/**
 * Abstract IntSet that throws an {@link UnsupportedOperationException} for all write operations on an IntSet.
 * <p>
 * It is up to implementors to ensure that {@link java.util.PrimitiveIterator.OfInt#remove()} throws the same exception.
 * @author wburns
 * @since 9.3
 */
abstract class AbstractImmutableIntSet implements IntSet {
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
   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(IntSet set) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(IntSet c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean add(Integer integer) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean addAll(IntSet set) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean addAll(Collection<? extends Integer> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeIf(Predicate<? super Integer> filter) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeIf(IntPredicate filter) {
      throw new UnsupportedOperationException();
   }
}
