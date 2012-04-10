/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util;

import net.jcip.annotations.Immutable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A lightweight, read-only copy of a List.  Typically used in place of the common idiom: <code> return
 * Collections.unmodifiableList(new ArrayList( myInternalList )); </code>
 * <p/>
 * a it is far more efficient than making a defensive copy and then wrapping the defensive copy in a read-only wrapper.
 * <p/>
 * Also used whenever a read-only reference List is needed.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Immutable
public class ImmutableListCopy<E> extends AbstractList<E> implements Externalizable, Immutables.Immutable {
   private static final long serialVersionUID = 10929568968966L;
   private E[] elements;
   private int size;

   /**
    * Constructs a new ImmutableListCopy. Required by Serialization.
    */
   public ImmutableListCopy() {
   }

   /**
    * Only one copy constructor since the list is immutable.
    *
    * @param c collection to copy from
    */
   @SuppressWarnings("unchecked")
   public ImmutableListCopy(Collection<? extends E> c) {
      size = c.size();
      Object[] el = new Object[size]; // no room for growth;
      el = c.toArray(el);
      elements = (E[]) el;
   }

   /**
    * Assumes that the array passed in is "safe", i.e., is not referenced from elsewhere.  Use with care!
    *
    * @param array to reference
    */
   public ImmutableListCopy(E[] array) {
      size = array.length;
      elements = array;
   }

   /**
    * Utility constructors to allow combining collections
    *
    * @param collection1 collection to copy from
    * @param collection2 collection to copy from
    */
   @SuppressWarnings("unchecked")
   public ImmutableListCopy(Collection<? extends E> collection1, Collection<? extends E> collection2) {
      size = collection1.size() + collection2.size();
      elements = (E[]) new Object[size]; // no room for growth;
      Object[] c1 = new Object[collection1.size()];
      Object[] c2 = new Object[collection2.size()];
      c1 = collection1.toArray(c1);
      c2 = collection2.toArray(c2);
      System.arraycopy(c1, 0, elements, 0, c1.length);
      System.arraycopy(c2, 0, elements, c1.length, c2.length);
   }

   @Override
   public final int size() {
      return size;
   }

   @Override
   public final boolean isEmpty() {
      return size == 0;
   }

   @Override
   public final boolean contains(Object o) {
      return indexOf(o) >= 0;
   }

   @Override
   public final Iterator<E> iterator() {
      return new ImmutableIterator();
   }

   @Override
   public final Object[] toArray() {
      Object[] result = new Object[size];
      System.arraycopy(elements, 0, result, 0, size);
      return result;
   }

   @Override
   @SuppressWarnings("unchecked")
   public final <T> T[] toArray(T[] a) {
      if (a.length < size) {
         a = (T[]) Array.newInstance(a.getClass().getComponentType(), size);
      }
      System.arraycopy(elements, 0, a, 0, size);
      if (a.length > size) a[size] = null;
      return a;
   }

   @Override
   public final boolean add(E o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean remove(Object o) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean addAll(int index, Collection<? extends E> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public final boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
   }

   public final E get(int index) {
      if (index >= size || index < 0) throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
      return elements[index];
   }

   @Override
   public final int indexOf(Object o) {
      if (o == null) {
         for (int i = 0; i < size; i++) {
            if (elements[i] == null) return i;
         }
      } else {
         for (int i = 0; i < size; i++) {
            if (o.equals(elements[i])) return i;
         }
      }
      return -1;
   }

   @Override
   public final int lastIndexOf(Object o) {
      if (o == null) {
         for (int i = size - 1; i >= 0; i--) {
            if (elements[i] == null) return i;
         }
      } else {
         for (int i = size - 1; i >= 0; i--) {
            if (o.equals(elements[i])) return i;
         }
      }
      return -1;
   }

   @Override
   public final ListIterator<E> listIterator() {
      return new ImmutableIterator();
   }

   @Override
   public final ListIterator<E> listIterator(int index) {
      return new ImmutableIterator(index);
   }

   @Override
   public final List<E> subList(int fromIndex, int toIndex) {
      return new ImmutableSubList<E>(fromIndex, toIndex);
   }

   /**
    * Format: - entry array size (int) - elements (Object)
    *
    * @param out stream to write to
    * @throws IOException
    */
   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(size);
      for (E e : elements) out.writeObject(e);
   }

   /**
    * See {@link #writeExternal(java.io.ObjectOutput)} for serialization format
    *
    * @param in stream
    * @throws IOException
    * @throws ClassNotFoundException
    */
   @SuppressWarnings("unchecked")
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      size = in.readInt();
      elements = (E[]) new Object[size];
      for (int i = 0; i < size; i++) elements[i] = (E) in.readObject();
   }

   private class ImmutableIterator implements ListIterator<E> {
      int cursor = 0;

      ImmutableIterator(int index) {
         if (index < 0 || index > size()) throw new IndexOutOfBoundsException("Index: " + index);
         cursor = index;
      }

      ImmutableIterator() {
      }

      public boolean hasNext() {
         return cursor != size;
      }

      public E next() {
         try {
            return get(cursor++);
         }
         catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException();
         }
      }

      public void remove() {
         throw new UnsupportedOperationException();
      }

      public boolean hasPrevious() {
         return cursor != 0;
      }

      public E previous() {
         try {
            return get(--cursor);
         }
         catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException();
         }
      }

      public int nextIndex() {
         return cursor;
      }

      public int previousIndex() {
         return cursor - 1;
      }

      public void set(E o) {
         throw new UnsupportedOperationException();
      }

      public void add(E o) {
         throw new UnsupportedOperationException();
      }
   }

   private class ImmutableSubList<E> extends AbstractList<E> {
      private int offset;
      private int size;

      ImmutableSubList(int fromIndex, int toIndex) {
         if (fromIndex < 0 || toIndex > ImmutableListCopy.this.size || fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex + "), toIndex(" + toIndex + "), size (" + ImmutableListCopy.this.size + "), List=" + ImmutableListCopy.this.toString());
         offset = fromIndex;
         size = toIndex - fromIndex;
      }

      @SuppressWarnings("unchecked")
      public final E get(int index) {
         if (index < 0 || index >= size) throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
         return (E) ImmutableListCopy.this.get(index + offset);
      }

      public final int size() {
         return size;
      }

      @Override
      protected final void removeRange(int fromIndex, int toIndex) {
         throw new UnsupportedOperationException();
      }

      @Override
      public final boolean addAll(Collection<? extends E> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public final boolean addAll(int index, Collection<? extends E> c) {
         throw new UnsupportedOperationException();
      }

      @Override
      public final Iterator<E> iterator() {
         return super.listIterator();
      }

      @Override
      public final ListIterator<E> listIterator(final int index) {
         if (index < 0 || (index != 0 && index >= size))
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);

         return new ListIterator<E>() {
            private ListIterator<?> i = ImmutableListCopy.this.listIterator(index + offset);

            public boolean hasNext() {
               return nextIndex() < size;
            }

            @SuppressWarnings("unchecked")
            public E next() {
               if (hasNext())
                  return (E) i.next();
               else
                  throw new NoSuchElementException();
            }

            public boolean hasPrevious() {
               return previousIndex() >= 0;
            }

            @SuppressWarnings("unchecked")
            public E previous() {
               if (hasPrevious())
                  return (E) i.previous();
               else
                  throw new NoSuchElementException();
            }

            public int nextIndex() {
               return i.nextIndex() - offset;
            }

            public int previousIndex() {
               return i.previousIndex() - offset;
            }

            public void remove() {
               throw new UnsupportedOperationException();
            }

            public void set(E o) {
               throw new UnsupportedOperationException();
            }

            public void add(E o) {
               throw new UnsupportedOperationException();
            }
         };
      }

      @Override
      public final List<E> subList(int fromIndex, int toIndex) {
         return new ImmutableSubList<E>(offset + fromIndex, offset + toIndex);
      }
   }
}
