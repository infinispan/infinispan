package org.infinispan.commands.read;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Abstract collection that uses an underlying Cache instance to do various operations.  This is useful for a backing
 * collection such as entrySet, keySet or values from the Map interface.  Implementors only need to implement individual
 * methods such as {@link Collection#contains(Object)}, {@link Collection#remove(Object)} and
 * {@link org.infinispan.commons.util.CloseableIteratorCollection#iterator()}.  The {@link Collection#add(Object)} by default will throw an
 * {@link java.lang.UnsupportedOperationException}.
 *
 * @author wburns
 * @since 7.0
 */
public abstract class AbstractCloseableIteratorCollection<O, K, V> extends AbstractCollection<O> implements CloseableIteratorCollection<O> {
   protected final Cache<K, V> cache;

   public AbstractCloseableIteratorCollection(Cache<K, V> cache) {
      this.cache = cache;
   }

   public static interface IteratorConverter {
      Object forEntry(CacheEntry entry);
   }

   @Override
   public abstract CloseableIterator<O> iterator();

   @Override
   public abstract boolean contains(Object o);

   @Override
   public abstract boolean remove(Object o);

   @Override
   public int size() {
      return cache.size();
   }

   @Override
   public boolean isEmpty() {
      return cache.isEmpty();
   }

   // Copied from AbstractCollection since we need to close iterator
   @Override
   public Object[] toArray() {
      // Estimate size of array; be prepared to see more or fewer elements
      Object[] r = new Object[size()];
      try (CloseableIterator<O> it = iterator()) {
         for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) // fewer elements than expected
               return Arrays.copyOf(r, i);
            r[i] = it.next();
         }
         return it.hasNext() ? finishToArray(r, it) : r;
      }
   }

   // Copied from AbstractCollection since we need to close iterator
   @Override
   public <T> T[] toArray(T[] a) {
      // Estimate size of array; be prepared to see more or fewer elements
      int size = size();
      T[] r = a.length >= size ? a :
            (T[])java.lang.reflect.Array
                  .newInstance(a.getClass().getComponentType(), size);
      try (CloseableIterator<O> it = iterator()) {
         for (int i = 0; i < r.length; i++) {
            if (! it.hasNext()) { // fewer elements than expected
               if (a == r) {
                  r[i] = null; // null-terminate
               } else if (a.length < i) {
                  return Arrays.copyOf(r, i);
               } else {
                  System.arraycopy(r, 0, a, 0, i);
                  if (a.length > i) {
                     a[i] = null;
                  }
               }
               return a;
            }
            r[i] = (T)it.next();
         }
         // more elements than expected
         return it.hasNext() ? finishToArray(r, it) : r;
      }
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      boolean modified = false;
      for (Object o : c) {
         if (remove(o)) {
            modified = true;
         }
      }
      return modified;
   }

   // Copied from AbstractCollection since we need to close iterator
   @Override
   public boolean retainAll(Collection<?> c) {
      boolean modified = false;
      try (CloseableIterator<O> it = iterator()) {
         while (it.hasNext()) {
            if (!c.contains(it.next())) {
               it.remove();
               modified = true;
            }
         }
         return modified;
      }
   }

   @Override
   public void clear() {
      cache.clear();
   }

   // Copied from AbstractCollection to support toArray methods
   @SuppressWarnings("unchecked")
   private static <T> T[] finishToArray(T[] r, Iterator<?> it) {
      int i = r.length;
      while (it.hasNext()) {
         int cap = r.length;
         if (i == cap) {
            int newCap = cap + (cap >> 1) + 1;
            // overflow-conscious code
            if (newCap - MAX_ARRAY_SIZE > 0)
               newCap = hugeCapacity(cap + 1);
            r = Arrays.copyOf(r, newCap);
         }
         r[i++] = (T)it.next();
      }
      // trim if overallocated
      return (i == r.length) ? r : Arrays.copyOf(r, i);
   }

   // Copied from AbstractCollection to support toArray methods
   private static int hugeCapacity(int minCapacity) {
      if (minCapacity < 0) // overflow
         throw new OutOfMemoryError
               ("Required array size too large");
      return (minCapacity > MAX_ARRAY_SIZE) ?
            Integer.MAX_VALUE :
            MAX_ARRAY_SIZE;
   }

   private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
}
