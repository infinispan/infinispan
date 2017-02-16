package org.infinispan.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Assumes that the delegate collection already contains unique elements.
 */
public class ReadOnlyCollectionAsSet<T> implements Set<T> {
   private final Collection<? extends T> delegate;

   public ReadOnlyCollectionAsSet(Collection<? extends T> delegate) {
      this.delegate = delegate;
   }

   @Override
   public int size() {
      return delegate.size();
   }

   @Override
   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return delegate.contains(o);
   }

   @Override
   public Iterator<T> iterator() {
      return new Iterator<T>() {
         private final Iterator<? extends T> it = delegate.iterator();

         @Override
         public boolean hasNext() {
            return it.hasNext();
         }

         @Override
         public T next() {
            return it.next();
         }
      };
   }

   @Override
   public Object[] toArray() {
      return delegate.toArray();
   }

   @Override
   public <T1> T1[] toArray(T1[] a) {
      return delegate.toArray(a);
   }

   @Override
   public boolean add(T t) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return delegate.containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }
}
