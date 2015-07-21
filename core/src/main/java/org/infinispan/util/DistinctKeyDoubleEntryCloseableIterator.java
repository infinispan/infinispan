package org.infinispan.util;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

public class DistinctKeyDoubleEntryCloseableIterator<E, K> implements CloseableIterator<E> {
   private final CloseableIterator<E> iterator1;
   private final CloseableIterator<E> iterator2;
   private final Function<? super E, K> function;
   private final Set<K> keysSeenInFirst;

   private boolean completedFirst = false;
   private E iterator2Next;

   public DistinctKeyDoubleEntryCloseableIterator(CloseableIterator<E> first,
                                                  CloseableIterator<E> second,
                                                  Function<? super E, K> function,
                                                  Set<K> seenKeys) {
      this.iterator1 = first;
      this.iterator2 = second;
      this.function = function;
      this.keysSeenInFirst = seenKeys;
   }

   @Override
   public void close() {
      try {
         iterator1.close();
      } catch (Throwable t1) {
         try {
            iterator2.close();
         } catch (Throwable t2) {
            t1.addSuppressed(t2);
         }
         throw t1;
      }
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
         E e = iterator2.next();
         if (!keysSeenInFirst.remove(function.apply(e))) {
            iterator2Next = e;
            break;
         }
      }
      return iterator2Next != null;
   }

   @Override
   public E next() {
      E next;
      if (!completedFirst) {
         // We have to double check hasNext in case if they are calling next without hasNext
         if (iterator1.hasNext()) {
            next = iterator1.next();
            keysSeenInFirst.add(function.apply(next));
            return next;
         } else {
            completedFirst = true;
         }
      }

      E e;
      if (iterator2Next != null) {
         e = iterator2Next;
         iterator2Next = null;
         return e;
      }
      while(iterator2.hasNext()) {
         e = iterator2.next();
         if (!keysSeenInFirst.remove(function.apply(e))) {
            return e;
         }
      }
      throw new NoSuchElementException();
   }
}
