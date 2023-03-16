package org.infinispan.query.core.impl;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;

import org.infinispan.commons.util.CloseableIterator;

public class MappingEntryIterator<K, S, T> implements CloseableIterator<T> {

   private final CloseableIterator<Map.Entry<K, S>> entryIterator;
   private final BiFunction<K, S, T> mapper;

   private long skip = 0;
   private long max = -1;

   private T current;
   private long index;

   public MappingEntryIterator(CloseableIterator<Map.Entry<K, S>> entryIterator, BiFunction<K, S, T> mapper) {
      this.entryIterator = entryIterator;
      this.mapper = mapper;
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
      while (current == null && entryIterator.hasNext()) {
         Map.Entry<K, S> next = entryIterator.next();
         T mapped = transform(next.getKey(), next.getValue());
         if (mapped != null) {
            index++;
         }
         if (index > skip && (max == -1 || index <= skip + max)) {
            current = mapped;
         }
      }
   }

   private T transform(K k, S s) {
      if (s == null) {
         return null;
      }
      if (mapper == null) {
         return (T) s;
      }

      return mapper.apply(k, s);
   }

   public MappingEntryIterator<K, S, T> skip(long skip) {
      this.skip = skip;
      return this;
   }

   public MappingEntryIterator<K, S, T> limit(long max) {
      this.max = max;
      return this;
   }

   @Override
   public void close() {
      entryIterator.close();
   }
}
