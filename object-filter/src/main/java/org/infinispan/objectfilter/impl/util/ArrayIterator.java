package org.infinispan.objectfilter.impl.util;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An immutable {@link Iterator} for arrays.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ArrayIterator<T> implements Iterator<T> {

   /**
    * An array of whatever type.
    */
   private final Object array;

   /**
    * Current position.
    */
   private int pos = 0;

   ArrayIterator(Object array) {
      if (array == null) {
         throw new IllegalArgumentException("Argument cannot be null");
      }
      if (!array.getClass().isArray()) {
         throw new IllegalArgumentException("Argument is expected to be an array");
      }
      this.array = array;
   }

   @Override
   public boolean hasNext() {
      return pos < Array.getLength(array);
   }

   @Override
   public T next() {
      try {
         return (T) Array.get(array, pos++);
      } catch (ArrayIndexOutOfBoundsException e) {
         throw new NoSuchElementException(e.getMessage());
      }
   }
}
