package org.infinispan.objectfilter.impl.util;

import java.lang.reflect.Array;
import java.util.Iterator;

/**
 * An immutable Iterator for array objects.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ArrayIterator implements Iterator<Object> {

   /**
    * An array of whatever type.
    */
   private final Object array;

   /**
    * Current position.
    */
   private int pos = 0;

   public ArrayIterator(Object array) {
      if (!array.getClass().isArray()) {
         throw new IllegalArgumentException("Argument is expected to be an array");
      }
      this.array = array;
   }

   public boolean hasNext() {
      return pos < Array.getLength(array);
   }

   public Object next() {
      return Array.get(array, pos++);
   }

   public void remove() {
      throw new UnsupportedOperationException("This iterator is immutable");
   }
}
