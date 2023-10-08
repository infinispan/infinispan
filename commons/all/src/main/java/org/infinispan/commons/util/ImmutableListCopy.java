package org.infinispan.commons.util;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import net.jcip.annotations.Immutable;


/**
 * A lightweight, read-only copy of a List.  Typically used in place of the common idiom: <code> return
 * Collections.unmodifiableList(new ArrayList( myInternalList )); </code>
  * It is far more efficient than making a defensive copy and then wrapping the defensive copy in a read-only wrapper.
  * Also used whenever a read-only reference List is needed.
  *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@Immutable
public class ImmutableListCopy<E> extends AbstractList<E> implements Immutables.Immutable {
   public static final Object[] EMPTY_ARRAY = new Object[0];

   private final Object[] elements;

   /**
    * Constructs an empty ImmutableListCopy.
    */
   public ImmutableListCopy() {
      elements = EMPTY_ARRAY;
   }

   /**
    * Only one copy constructor since the list is immutable.
    *
    * @param c collection to copy from
    */
   @SuppressWarnings("unchecked")
   public ImmutableListCopy(Collection<? extends E> c) {
      if (c instanceof ImmutableListCopy) {
         elements = ((ImmutableListCopy<? extends E>) c).elements;
      } else {
         elements = c.toArray();
      }
   }

   /**
    * Assumes that the array passed in is "safe", i.e., is not referenced from elsewhere.  Use with care!
    *
    * @param array to reference
    */
   public ImmutableListCopy(E[] array) {
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
      if (collection2.isEmpty()) {
         if (collection1 instanceof ImmutableListCopy) {
            elements = ((ImmutableListCopy<? extends E>) collection1).elements;
         } else {
            elements = collection1.toArray();
         }
      } else if (collection1.isEmpty()) {
         if (collection2 instanceof ImmutableListCopy) {
            elements = ((ImmutableListCopy<? extends E>) collection2).elements;
         } else {
            elements = collection2.toArray();
         }
      } else {
         int c1Size = collection1.size();
         int c2Size = collection2.size();
         int size = c1Size + c2Size;
         elements = new Object[size]; // no room for growth;
         collection1.toArray(elements);
         Object[] c2 = collection2.toArray();
         System.arraycopy(c2, 0, elements, c1Size, c2Size);
      }
   }

   @Override
   public final int size() {
      return elements.length;
   }

   @Override
   public final boolean isEmpty() {
      return elements.length == 0;
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
      return Arrays.copyOf(elements, elements.length);
   }

   @Override
   @SuppressWarnings("unchecked")
   public final <T> T[] toArray(T[] a) {
      int size = elements.length;
      if (a.length < size) {
         return (T[]) Arrays.copyOf(elements, size, a.getClass());
      }

      System.arraycopy(elements, 0, a, 0, size);
      if (a.length > size) {
         a[size] = null;
      }
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

   @SuppressWarnings("unchecked")
   @Override
   public final E get(int index) {
      return (E) elements[index];
   }

   @Override
   public final int indexOf(Object o) {
      int size = elements.length;
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
      int size = elements.length;
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
      return new ImmutableSubList(fromIndex, toIndex);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (!(o instanceof ImmutableListCopy))
         return super.equals(o);

      ImmutableListCopy<?> that = (ImmutableListCopy<?>) o;
      return Arrays.equals(elements, that.elements);

   }

   @Override
   public int hashCode() {
      return Arrays.hashCode(elements);
   }

   private class ImmutableIterator implements ListIterator<E> {
      int cursor = 0;

      ImmutableIterator(int index) {
         if (index < 0 || index > size()) throw new IndexOutOfBoundsException("Index: " + index);
         cursor = index;
      }

      ImmutableIterator() {
      }

      @Override
      public boolean hasNext() {
         return cursor != elements.length;
      }

      @Override
      public E next() {
         try {
            return get(cursor++);
         }
         catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException();
         }
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasPrevious() {
         return cursor != 0;
      }

      @Override
      public E previous() {
         try {
            return get(--cursor);
         }
         catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException();
         }
      }

      @Override
      public int nextIndex() {
         return cursor;
      }

      @Override
      public int previousIndex() {
         return cursor - 1;
      }

      @Override
      public void set(E o) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void add(E o) {
         throw new UnsupportedOperationException();
      }
   }

   public class ImmutableSubList extends AbstractList<E> {
      private final int offset;
      private final int size;

      ImmutableSubList(int fromIndex, int toIndex) {
         int size = ImmutableListCopy.this.elements.length;
         if (fromIndex < 0 || toIndex > size || fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex + "), toIndex(" + toIndex + "), size (" + size + "), List=" + ImmutableListCopy.this.toString());
         offset = fromIndex;
         this.size = toIndex - fromIndex;
      }

      @Override
      public final E get(int index) {
         if (index < 0 || index >= size) throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
         return ImmutableListCopy.this.get(index + offset);
      }

      @Override
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
            private final ListIterator<?> i = ImmutableListCopy.this.listIterator(index + offset);

            @Override
            public boolean hasNext() {
               return nextIndex() < size;
            }

            @Override
            @SuppressWarnings("unchecked")
            public E next() {
               if (hasNext())
                  return (E) i.next();
               else
                  throw new NoSuchElementException();
            }

            @Override
            public boolean hasPrevious() {
               return previousIndex() >= 0;
            }

            @Override
            @SuppressWarnings("unchecked")
            public E previous() {
               if (hasPrevious())
                  return (E) i.previous();
               else
                  throw new NoSuchElementException();
            }

            @Override
            public int nextIndex() {
               return i.nextIndex() - offset;
            }

            @Override
            public int previousIndex() {
               return i.previousIndex() - offset;
            }

            @Override
            public void remove() {
               throw new UnsupportedOperationException();
            }

            @Override
            public void set(E o) {
               throw new UnsupportedOperationException();
            }

            @Override
            public void add(E o) {
               throw new UnsupportedOperationException();
            }
         };
      }

      @Override
      public final List<E> subList(int fromIndex, int toIndex) {
         return new ImmutableSubList(offset + fromIndex, offset + toIndex);
      }
   }

}
