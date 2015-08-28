package org.infinispan.commons.util;

import java.util.Collection;

/**
 * Adapts {@link java.util.Collection} to {@link CloseableIteratorCollection}
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class CloseableIteratorCollectionAdapter<E> implements CloseableIteratorCollection<E> {
   protected final Collection<E> delegate;

   public CloseableIteratorCollectionAdapter(Collection<E> delegate) {
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
   public CloseableIterator<E> iterator() {
      return Closeables.iterator(delegate.iterator());
   }

   @Override
   public CloseableSpliterator<E> spliterator() {
      return Closeables.spliterator(delegate.spliterator());
   }

   @Override
   public Object[] toArray() {
      return delegate.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return delegate.toArray(a);
   }

   @Override
   public boolean add(E e) {
      return delegate.add(e);
   }

   @Override
   public boolean remove(Object o) {
      return delegate.remove(o);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return delegate.containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      return delegate.addAll(c);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return delegate.retainAll(c);
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return delegate.removeAll(c);
   }

   @Override
   public void clear() {
      delegate.clear();
   }
}
