package org.infinispan.objectfilter.impl.util;

import java.lang.reflect.Array;
import java.util.Iterator;

/**
 * An immutable Iterator for arrays.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ArrayIterator<T> implements Iterator<T> {

   /**
    * An array of whatever type.
    */
   private final Object array;

   /**
    * Current position.
    */
   private int pos = 0;

   public ArrayIterator(Object array) {
      if (array == null) {
         throw new IllegalArgumentException("Argument cannot be null");
      }
      if (!array.getClass().isArray()) {
         throw new IllegalArgumentException("Argument is expected to be an array");
      }
      this.array = array;
   }

   public boolean hasNext() {
      return pos < Array.getLength(array);
   }

   public T next() {
      return (T) Array.get(array, pos++);
   }

   public void remove() {
      throw new UnsupportedOperationException("This iterator is immutable");
   }
}
