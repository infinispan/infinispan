package org.infinispan.query.core.impl;

import java.util.NoSuchElementException;
import java.util.function.Function;

import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.util.CloseableIterator;

/**
 * A {@link CloseableIterator} decorator that can be sliced and have its elements transformed.
 *
 * @param <S> Type of the original iterator
 * @param <T> Resulting type
 * @since 11.0
 */
public final class MappingIterator<S, T> implements ClosableIteratorWithCount<T> {

   private final CloseableIterator<S> iterator;
   private final Function<? super S, ? extends T> mapper;
   private final HitCount count;

   private long skip = 0;
   private long max = -1;

   private T current;
   private long index;

   public MappingIterator(CloseableIterator<S> iterator) {
      this(iterator, null);
   }

   public MappingIterator(CloseableIterator<S> iterator, Function<? super S, ? extends T> mapper) {
      this(iterator, mapper, null);
   }

   public MappingIterator(CloseableIterator<S> iterator, Function<? super S, ? extends T> mapper, HitCount count) {
      this.iterator = iterator;
      this.mapper = mapper;
      this.count = count;
   }

   @Override
   public boolean hasNext() {
      updateNext();
      return current != null;
   }

   @Override
   public T next() {
      if (hasNext()) {
         T element = current;
         current = null;
         return element;
      } else {
         throw new NoSuchElementException();
      }
   }

   private void updateNext() {
      while (current == null && iterator.hasNext()) {
         T mapped = transform(iterator.next());
         if (mapped != null) {
            index++;
         }
         if (index > skip && (max == -1 || index <= skip + max)) {
            current = mapped;
         }
      }
   }

   private T transform(S s) {
      if (s == null) {
         return null;
      }
      if (mapper == null) {
         return (T) s;
      }

      return mapper.apply(s);
   }

   public MappingIterator<S, T> skip(long skip) {
      this.skip = skip;
      return this;
   }

   public MappingIterator<S, T> limit(long max) {
      this.max = max;
      return this;
   }

   @Override
   public void close() {
      iterator.close();
   }

   @Override
   public HitCount count() {
      return count;
   }
}
