package org.infinispan.query.impl;

import org.infinispan.Cache;
import org.infinispan.query.QueryIterator;

/**
 * // TODO: Document this
 * <p/>
 * This is the abstract superclass of the 2 iterators. Since some of the methods have the same implementations they have
 * been put onto a separate class.
 *
 * @author Navin Surtani
 * @since 4.0
 */


public abstract class AbstractIterator implements QueryIterator {

   protected Object[] buffer;
   protected Cache cache;

   protected int index = 0;
   protected int bufferIndex = -1;
   protected int max;
   protected int first;
   protected int fetchSize;

   public void first() {
      index = first;
   }

   public void last() {
      index = max;
   }

   public void afterFirst() {
      index = first + 1;
   }

   public void beforeLast() {
      index = max - 1;
   }

   public boolean isFirst() {
      return index == first;
   }

   public boolean isLast() {
      return index == max;
   }

   public boolean isAfterFirst() {
      return index == first + 1;
   }

   public boolean isBeforeLast() {
      return index == max - 1;
   }

   public boolean hasPrevious() {
      return index >= first;
   }

   public boolean hasNext() {
      return index <= max;
   }
   
}
