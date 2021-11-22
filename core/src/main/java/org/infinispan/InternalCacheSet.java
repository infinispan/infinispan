package org.infinispan;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.IntSet;
import org.reactivestreams.Publisher;

/**
 * Base class for internal classes used in cache collections.
 *
 * <p>It extends {@link CacheSet} because that's what interceptors used return for
 * {@link org.infinispan.commands.read.KeySetCommand} and {@link org.infinispan.commands.read.EntrySetCommand},
 * but because these classes are only used internally, we can avoid implementing most of the methods.</p>
 *
 * <p>Subclasses only need to implement {@link #localPublisher(IntSet)} and {@link #localPublisher(int)},
 * and a facade class like {@link org.infinispan.cache.impl.CacheBackedKeySet} implements the rest of the
 * {@link CacheSet} methods.</p>
 *
 * @param <E> The element type
 * @since 14.0
 * @author Dan Berindei
 */
public abstract class InternalCacheSet<E> implements CacheSet<E> {
   @Override
   public abstract Publisher<E> localPublisher(int segment);

   @Override
   public abstract Publisher<E> localPublisher(IntSet segments);

   @Override
   public final boolean removeIf(Predicate<? super E> filter) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void forEach(Consumer<? super E> action) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final CloseableIterator<E> iterator() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final CloseableSpliterator<E> spliterator() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final CacheStream<E> stream() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final CacheStream<E> parallelStream() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final int size() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean isEmpty() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean contains(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final Object[] toArray() {
      throw new UnsupportedOperationException();
   }

   @Override
   public final <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean add(E e) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final void clear() {
      throw new UnsupportedOperationException();
   }
}
