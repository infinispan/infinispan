package org.infinispan.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractDelegatingSet<E> implements Set<E> {

   protected abstract Set<E> delegate();

   @Override
   public int size() {
      return delegate().size();
   }

   @Override
   public boolean isEmpty() {
      return delegate().isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return delegate().contains(o);
   }

   @Override
   public Iterator<E> iterator() {
      return delegate().iterator();
   }

   @Override
   public Object[] toArray() {
      return delegate().toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return delegate().toArray(a);
   }

   @Override
   public boolean add(E e) {
      return delegate().add(e);
   }

   @Override
   public boolean remove(Object o) {
      return delegate().remove(o);
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return delegate().containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      return delegate().addAll(c);
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      return delegate().removeAll(c);
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      return delegate().removeAll(c);
   }

   @Override
   public void clear() {
      delegate().clear();
   }

}
