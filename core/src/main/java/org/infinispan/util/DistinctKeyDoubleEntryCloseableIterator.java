package org.infinispan.util;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class DistinctKeyDoubleEntryCloseableIterator<K, V> implements CloseableIterator<CacheEntry<K, V>> {
   private final CloseableIterator<CacheEntry<K, V>> iterator1;
   private final CloseableIterator<CacheEntry<K, V>> iterator2;
   private final Set<K> keysSeenInFirst;

   private boolean completedFirst = false;
   private CacheEntry<K, V> iterator2Next;

   public DistinctKeyDoubleEntryCloseableIterator(CloseableIterator<CacheEntry<K, V>> first,
                                                  CloseableIterator<CacheEntry<K, V>> second) {
      this.iterator1 = first;
      this.iterator2 = second;
      this.keysSeenInFirst = new HashSet<>();
   }

   public DistinctKeyDoubleEntryCloseableIterator(CloseableIterator<CacheEntry<K, V>> first,
                                                  CloseableIterator<CacheEntry<K, V>> second,
                                                  int estimateFirstSize) {
      this.iterator1 = first;
      this.iterator2 = second;
      this.keysSeenInFirst = new HashSet<>(estimateFirstSize);
   }

   @Override
   public void close() {
      iterator1.close();
      iterator2.close();
   }

   @Override
   public boolean hasNext() {
      boolean hasNext;
      if (!completedFirst) {
         hasNext = iterator1.hasNext();
         if (hasNext) {
            return hasNext;
         } else {
            completedFirst = true;
         }
      }

      while (iterator2.hasNext()) {
         CacheEntry<K, V> e = iterator2.next();
         if (!keysSeenInFirst.remove(e.getKey())) {
            iterator2Next = e;
            break;
         }
      }
      return iterator2Next != null;
   }

   @Override
   public CacheEntry<K, V> next() {
      CacheEntry<K, V> next;
      if (!completedFirst) {
         // We have to double check hasNext in case if they are calling next without hasNext
         if (iterator1.hasNext()) {
            next = iterator1.next();
            keysSeenInFirst.add(next.getKey());
            return next;
         } else {
            completedFirst = true;
         }
      }

      CacheEntry<K, V> e;
      if (iterator2Next != null) {
         e = iterator2Next;
         iterator2Next = null;
         return e;
      }
      while((e = iterator2.next()) != null) {
         if (!keysSeenInFirst.remove(e)) {
            return e;
         }
      }
      throw new NoSuchElementException();
   }
}
