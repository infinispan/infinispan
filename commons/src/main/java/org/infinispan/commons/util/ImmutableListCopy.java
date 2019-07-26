package org.infinispan.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;

import net.jcip.annotations.Immutable;


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
public class ImmutableListCopy<E> extends AbstractList<E> implements Immutables.Immutable {
   private E[] elements;

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
      int size = c.size();
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
      int size = collection1.size() + collection2.size();
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
      int size = elements.length;
      Object[] result = new Object[size];
      System.arraycopy(elements, 0, result, 0, size);
      return result;
   }

   @Override
   @SuppressWarnings("unchecked")
   public final <T> T[] toArray(T[] a) {
      int size = elements.length;
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

   @Override
   public final E get(int index) {
      return elements[index];
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
      return new ImmutableSubList<E>(fromIndex, toIndex);
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
      int result = super.hashCode();
      result = 31 * result + Arrays.hashCode(elements);
      return result;
   }

   public static class Externalizer implements AdvancedExternalizer<ImmutableListCopy> {

      @Override
      public Integer getId() {
         return Ids.IMMUTABLE_LIST_COPY;
      }

      @Override
      public Set<Class<? extends ImmutableListCopy>> getTypeClasses() {
         return Util.asSet(ImmutableListCopy.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ImmutableListCopy object) throws IOException {
         MarshallUtil.marshallArray(object.elements, output);
      }

      @Override
      public ImmutableListCopy readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object[] elements = MarshallUtil.unmarshallArray(input, Util::objectArray);
         return new ImmutableListCopy<>(elements);
      }
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

   public class ImmutableSubList<E> extends AbstractList<E> {
      private int offset;
      private int size;

      ImmutableSubList(int fromIndex, int toIndex) {
         int size = ImmutableListCopy.this.elements.length;
         if (fromIndex < 0 || toIndex > size || fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex + "), toIndex(" + toIndex + "), size (" + size + "), List=" + ImmutableListCopy.this.toString());
         offset = fromIndex;
         this.size = toIndex - fromIndex;
      }

      @Override
      @SuppressWarnings("unchecked")
      public final E get(int index) {
         if (index < 0 || index >= size) throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
         return (E) ImmutableListCopy.this.get(index + offset);
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
            private ListIterator<?> i = ImmutableListCopy.this.listIterator(index + offset);

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
         return new ImmutableSubList<E>(offset + fromIndex, offset + toIndex);
      }
   }

}
