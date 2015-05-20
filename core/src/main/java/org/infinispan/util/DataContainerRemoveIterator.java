package org.infinispan.util;

import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * This is an iterator that will iterate upon the data container.  A cache is also provided to be used
 * when the remove method on the iterator is invoked.  Note that this means it will take part of any
 * ongoing transaction if there is one.
 */
public class DataContainerRemoveIterator<K, V> implements Iterator<CacheEntry<K, V>> {
   private final Cache<K, V> cache;
   private final Iterator<InternalCacheEntry<K, V>> dataContainerIterator;

   private K previousKey;

   public DataContainerRemoveIterator(Cache<K, V> cache) {
      this(cache, cache.getAdvancedCache().getDataContainer());
   }

   public DataContainerRemoveIterator(Cache<K, V> cache, DataContainer<K, V> dataContainer) {
      if (cache == null || dataContainer == null) {
         throw new NullPointerException();
      }
      this.cache = cache;
      this.dataContainerIterator = dataContainer.iterator();
   }

   @Override
   public boolean hasNext() {
      return dataContainerIterator.hasNext();
   }

   @Override
   public CacheEntry<K, V> next() {
      CacheEntry<K, V> entry = dataContainerIterator.next();
      previousKey = entry.getKey();
      return entry;
   }

   @Override
   public void remove() {
      if (previousKey == null) {
         throw new IllegalStateException();
      }
      cache.remove(previousKey);
      previousKey = null;
   }
}
