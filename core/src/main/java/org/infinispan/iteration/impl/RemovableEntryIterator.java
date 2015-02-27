package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;

import java.util.NoSuchElementException;

/**
 * A CloseableIterator implementation that allows for a CloseableIterator that doesn't allow remove operations to
 * implement remove by delegating the call to the provided cache to remove the previously read value.
 *
 * @author wburns
 * @since 7.0
 */
public class RemovableEntryIterator<K, V, C> implements CloseableIterator<CacheEntry<K, C>> {
   protected final CloseableIterator<CacheEntry<K, V>> realIterator;
   protected final Cache<K, V> cache;

   protected CacheEntry<K, C> previousValue;
   protected CacheEntry<K, C> currentValue;

   public RemovableEntryIterator(CloseableIterator<CacheEntry<K, V>> realIterator,
                                            Cache<K, V> cache, boolean initIterator) {
      this.realIterator = realIterator;
      this.cache = cache;
      if (initIterator) {
         currentValue = getNextFromIterator();
      }
   }

   protected CacheEntry<K, C> getNextFromIterator() {
      if (realIterator.hasNext()) {
         return (CacheEntry<K, C>) realIterator.next();
      } else {
         return null;
      }
   }

   @Override
   public boolean hasNext() {
      return currentValue != null;
   }

   @Override
   public CacheEntry<K, C> next() {
      if (currentValue == null) {
         throw new NoSuchElementException();
      }
      previousValue = currentValue;
      // Set the current value to the next one if there is one
      currentValue = getNextFromIterator();
      return previousValue;
   }

   @Override
   public void remove() {
      if (previousValue == null) {
         throw new IllegalStateException();
      }
      cache.remove(previousValue.getKey());
      previousValue = null;
   }

   @Override
   public void close() {
      currentValue = null;
      previousValue = null;
      realIterator.close();
   }
}