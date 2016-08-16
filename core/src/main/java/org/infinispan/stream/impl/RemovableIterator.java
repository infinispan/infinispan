package org.infinispan.stream.impl;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.infinispan.Cache;

/**
 * An Iterator implementation that allows for a Iterator that doesn't allow remove operations to
 * implement remove by delegating the call to the provided cache to remove the previously read value.  The key used
 * to remove from the cache is determined by first applying the removeFunction to the value retrieved from the
 * iterator.
 *
 * @author wburns
 * @since 8.0
 */
public class RemovableIterator<K, C> implements Iterator<C> {
   protected final Iterator<C> realIterator;
   protected final Cache<K, ?> cache;
   protected final Function<? super C, K> removeFunction;

   protected C previousValue;
   protected C currentValue;

   public RemovableIterator(Iterator<C> realIterator, Cache<K, ?> cache,
           Function<? super C, K> removeFunction) {
      this.realIterator = realIterator;
      this.cache = cache;
      this.removeFunction = removeFunction;
   }

   protected C getNextFromIterator() {
      if (realIterator.hasNext()) {
         return realIterator.next();
      } else {
         return null;
      }
   }

   @Override
   public boolean hasNext() {
      return currentValue != null || (currentValue = getNextFromIterator()) != null;
   }

   @Override
   public C next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }
      previousValue = currentValue;
      currentValue = null;
      return previousValue;
   }

   @Override
   public void remove() {
      if (previousValue == null) {
         throw new IllegalStateException();
      }
      cache.remove(removeFunction.apply(previousValue));
      previousValue = null;
   }
}