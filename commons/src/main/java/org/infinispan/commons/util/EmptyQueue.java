package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import static java.util.Collections.emptyIterator;

/**
 * @author gustavonalle
 * @since 7.2
 */
public final class EmptyQueue<T> implements Queue<T> {

   @Override
   public int size() {
      return 0;
   }

   @Override
   public boolean isEmpty() {
      return true;
   }

   @Override
   public boolean contains(Object o) {
      return false;
   }

   @Override
   public Iterator<T> iterator() {
      return emptyIterator();
   }

   @Override
   public Object[] toArray() {
      return new Object[0];
   }

   @Override
   public <T1> T1[] toArray(T1[] a) {
      throw new UnsupportedOperationException();
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
      return false;
   }

   @Override
   public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean offer(T t) {
      throw new UnsupportedOperationException();
   }

   @Override
   public T remove() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T poll() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T element() {
      throw new UnsupportedOperationException();
   }

   @Override
   public T peek() {
      throw new UnsupportedOperationException();
   }
}
