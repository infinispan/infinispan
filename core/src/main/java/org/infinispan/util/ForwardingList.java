/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.infinispan.util;

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

   public void add(int index, E element) {
      delegate().add(index, element);
   }

   public boolean addAll(int index, Collection<? extends E> elements) {
      return delegate().addAll(index, elements);
   }

   public E get(int index) {
      return delegate().get(index);
   }

   public int indexOf(Object element) {
      return delegate().indexOf(element);
   }

   public int lastIndexOf(Object element) {
      return delegate().lastIndexOf(element);
   }

   public ListIterator<E> listIterator() {
      return delegate().listIterator();
   }

   public ListIterator<E> listIterator(int index) {
      return delegate().listIterator(index);
   }

   public E remove(int index) {
      return delegate().remove(index);
   }

   public E set(int index, E element) {
      return delegate().set(index, element);
   }

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

   public Iterator<E> iterator() {
      return delegate().iterator();
   }

   public int size() {
      return delegate().size();
   }

   public boolean removeAll(Collection<?> collection) {
      return delegate().removeAll(collection);
   }

   public boolean isEmpty() {
      return delegate().isEmpty();
   }

   public boolean contains(Object object) {
      return delegate().contains(object);
   }

   public Object[] toArray() {
      return delegate().toArray();
   }

   public <T> T[] toArray(T[] array) {
      return delegate().toArray(array);
   }

   public boolean add(E element) {
      return delegate().add(element);
   }

   public boolean remove(Object object) {
      return delegate().remove(object);
   }

   public boolean containsAll(Collection<?> collection) {
      return delegate().containsAll(collection);
   }

   public boolean addAll(Collection<? extends E> collection) {
      return delegate().addAll(collection);
   }

   public boolean retainAll(Collection<?> collection) {
      return delegate().retainAll(collection);
   }

   public void clear() {
      delegate().clear();
   }

}
