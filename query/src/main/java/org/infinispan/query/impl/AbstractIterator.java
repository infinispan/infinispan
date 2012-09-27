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
package org.infinispan.query.impl;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.query.QueryIterator;

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
public abstract class AbstractIterator implements QueryIterator {

   protected final Object[] buffer;

   /**
    * Index of the element that will be returned by next()
    */
   protected int index = 0;

   /**
    * The index at which the buffer starts (the global index of the element at buffer[0])
    */
   protected int bufferIndex = -1;

   protected int max;
   protected final int fetchSize;
   private final QueryResultLoader resultLoader;

   protected AbstractIterator(QueryResultLoader resultLoader, int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("Incorrect value for fetchSize passed. Your fetchSize is less than 1");
      }
      this.resultLoader = resultLoader;
      this.fetchSize = fetchSize;
      this.buffer = new Object[fetchSize];
   }

   @Override
   public void beforeFirst() {
      index = 0;
   }

   @Override
   public void afterLast() {
      index = max + 1;
   }

   @Override
   public boolean hasPrevious() {
      return index > 0;
   }

   @Override
   public boolean hasNext() {
      return index <= max;
   }

   /**
    * Returns the index of the element that would be returned by a subsequent call to next.
    *
    * @return Index of next element.
    */
   @Override
   public int nextIndex() {
      return index;
   }

   /**
    * Returns the index of the element that would be returned by a subsequent call to previous.
    *
    * @return Index of previous element.
    */
   @Override
   public int previousIndex() {
      return index - 1;
   }

   /**
    * This method is not supported and should not be used. Use cache.remove() instead.
    */
   @Override
   public void remove() {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   /**
    * This method is not supported in and should not be called. Use cache.put() instead.
    *
    * @param o
    * @throws UnsupportedOperationException
    */
   @Override
   public void set(Object o) throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache.  Please use searchableCache.put()");
   }

   /**
    * This method is not supported in and should not be called. Use cache.put() instead.
    *
    * @param o
    * @throws UnsupportedOperationException
    */
   @Override
   public void add(Object o) {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache. Please use searchableCache.put()");
   }

   /**
    * Jumps to a given index in the list of results.
    *
    * @param index to jump to
    * @throws IndexOutOfBoundsException
    */
   @Override
   public void jumpToIndex(int index) throws IndexOutOfBoundsException {
      if (index > max || index < 0) {
         throw new IndexOutOfBoundsException("The index you entered is either greater than the size of the list or negative");
      }
      this.index = index;
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

   @Override
   public Object previous() {
      if (!hasPrevious()) throw new NoSuchElementException("Index is out of bounds. There is no previous");

      index--;

      if (mustInitializeBuffer()) {
         int startIndex = Math.max(0, index - (buffer.length - 1));
         fillBuffer(startIndex);
      }

      return buffer[index - bufferIndex];
   }

   private boolean mustInitializeBuffer() {
      return bufferIndex == -1                          // buffer init check
            || index < bufferIndex                      // lower boundary
            || index >= (bufferIndex + buffer.length);  // upper boundary
   }

   private void fillBuffer(int startIndex) {
      bufferIndex = startIndex;
      int resultsToLoad = Math.min( buffer.length, max + 1 - bufferIndex);
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
