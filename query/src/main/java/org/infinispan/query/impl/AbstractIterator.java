package org.infinispan.query.impl;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.query.ResultIterator;

import java.util.NoSuchElementException;

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
public abstract class AbstractIterator implements ResultIterator {

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
         throw new IllegalArgumentException("Incorrect value for fetchSize passed. Your fetchSize is less than 1");
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

   /**
    * This method is not supported and should not be used. Use cache.remove() instead.
    */
   @Override
   public void remove() {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   @Override
   public Object next() {
      if (!hasNext()) throw new NoSuchElementException("Out of boundaries. There is no next");

      if (mustInitializeBuffer()) {
         fillBuffer(index);
      }

      int indexToReturn = index - bufferIndex;
      index++;
      return buffer[indexToReturn];
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
         buffer[i] = loadResult(bufferIndex + i);
      }
   }

   private Object loadResult(int index) {
      EntityInfo entityInfo = loadEntityInfo(index);
      return resultLoader.load(entityInfo);
   }

   protected abstract EntityInfo loadEntityInfo(int index);
}
