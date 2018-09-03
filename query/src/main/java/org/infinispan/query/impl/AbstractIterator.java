package org.infinispan.query.impl;

import java.util.NoSuchElementException;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.query.ResultIterator;

/**
 * This is the abstract superclass of the 2 iterators. Since some of the methods have the same implementations they have
 * been put onto a separate class.
 *
 * @author Navin Surtani
 * @author Marko Luksa
 * @see org.infinispan.query.impl.EagerIterator
 * @see org.infinispan.query.impl.LazyIterator
 * @since 4.0
 */
abstract class AbstractIterator<E> implements ResultIterator<E> {

   private final Object[] buffer;

   /**
    * Index of the element that will be returned by next()
    */
   private int index = 0;

   /**
    * The index at which the buffer starts (the global index of the element at buffer[0])
    */
   private int bufferIndex = -1;

   /**
    * The index of the last element that will be returned by this iterator.
    */
   private final int lastIndex;

   private final QueryResultLoader resultLoader;

   protected AbstractIterator(QueryResultLoader resultLoader, int firstIndex, int lastIndex, int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("fetchSize should be greater than 0");
      }
      this.resultLoader = resultLoader;
      this.index = firstIndex;
      this.lastIndex = lastIndex;

      int resultCount = lastIndex == -1 ? 0 : (lastIndex + 1 - firstIndex);
      this.buffer = new Object[Math.min(fetchSize, resultCount)]; // don't allocate more than necessary
   }

   @Override
   public boolean hasNext() {
      return index <= lastIndex;
   }

   @Override
   public E next() {
      if (!hasNext()) {
         throw new NoSuchElementException();
      }

      if (mustInitializeBuffer()) {
         fillBuffer(index);
      }

      int indexToReturn = index - bufferIndex;
      index++;
      return (E) buffer[indexToReturn];
   }

   private boolean mustInitializeBuffer() {
      return bufferIndex == -1                          // buffer init check
            || index < bufferIndex                      // lower boundary
            || index >= (bufferIndex + buffer.length);  // upper boundary
   }

   private void fillBuffer(int startIndex) {
      bufferIndex = startIndex;
      int resultsToLoad = Math.min(buffer.length, lastIndex + 1 - bufferIndex);
      for (int i = 0; i < resultsToLoad; i++) {
         EntityInfo entityInfo = loadEntityInfo(bufferIndex + i);
         buffer[i] = resultLoader.load(entityInfo);
      }
   }

   protected abstract EntityInfo loadEntityInfo(int index);
}
