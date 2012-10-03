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
   public boolean hasNext() {
      return index <= max;
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
