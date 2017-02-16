package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A list which forwards all its method calls to another list. Subclasses should override one or more methods to modify the
 * behavior of the backing list as desired per the <a href="http://en.wikipedia.org/wiki/Decorator_pattern">decorator
 * pattern</a>.
 *
 * <p>
 * This class does not implement {@link java.util.RandomAccess}. If the delegate supports random access, the
 * {@code ForwardingList} subclass should implement the {@code RandomAccess} interface.
 *
 * @author Mike Bostock
 * @since 2 (imported from Google Collections Library)
 */
public abstract class ForwardingList<E> implements List<E> {

   /** Constructor for use by subclasses. */
   protected ForwardingList() {
   }

   protected abstract List<E> delegate();

   @Override
   public void add(int index, E element) {
      delegate().add(index, element);
   }

   @Override
   public boolean addAll(int index, Collection<? extends E> elements) {
      return delegate().addAll(index, elements);
   }

   @Override
   public E get(int index) {
      return delegate().get(index);
   }

   @Override
   public int indexOf(Object element) {
      return delegate().indexOf(element);
   }

   @Override
   public int lastIndexOf(Object element) {
      return delegate().lastIndexOf(element);
   }

   @Override
   public ListIterator<E> listIterator() {
      return delegate().listIterator();
   }

   @Override
   public ListIterator<E> listIterator(int index) {
      return delegate().listIterator(index);
   }

   @Override
   public E remove(int index) {
      return delegate().remove(index);
   }

   @Override
   public E set(int index, E element) {
      return delegate().set(index, element);
   }

   @Override
   public List<E> subList(int fromIndex, int toIndex) {
      return delegate().subList(fromIndex, toIndex);
   }

   @Override
   public boolean equals(Object object) {
      return object == this || delegate().equals(object);
   }

   @Override
   public int hashCode() {
      return delegate().hashCode();
   }

   @Override
   public Iterator<E> iterator() {
      return delegate().iterator();
   }

   @Override
   public int size() {
      return delegate().size();
   }

   @Override
   public boolean removeAll(Collection<?> collection) {
      return delegate().removeAll(collection);
   }

   @Override
   public boolean isEmpty() {
      return delegate().isEmpty();
   }

   @Override
   public boolean contains(Object object) {
      return delegate().contains(object);
   }

   @Override
   public Object[] toArray() {
      return delegate().toArray();
   }

   @Override
   public <T> T[] toArray(T[] array) {
      return delegate().toArray(array);
   }

   @Override
   public boolean add(E element) {
      return delegate().add(element);
   }

   @Override
   public boolean remove(Object object) {
      return delegate().remove(object);
   }

   @Override
   public boolean containsAll(Collection<?> collection) {
      return delegate().containsAll(collection);
   }

   @Override
   public boolean addAll(Collection<? extends E> collection) {
      return delegate().addAll(collection);
   }

   @Override
   public boolean retainAll(Collection<?> collection) {
      return delegate().retainAll(collection);
   }

   @Override
   public void clear() {
      delegate().clear();
   }

}
