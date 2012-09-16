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

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import net.jcip.annotations.NotThreadSafe;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This is the implementation class for the interface QueryResultIterator which extends ListIterator. It is what is
 * returned when the {@link org.infinispan.query.CacheQuery#iterator()}.
 * <p/>
 * <p/>
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */
@NotThreadSafe
public class EagerIterator extends AbstractIterator {
   //private final int size;
   private List<EntityInfo> entityInfos;
   private QueryResultLoader resultLoader;

   private static final Log log = LogFactory.getLog(EagerIterator.class);


   public EagerIterator(List<EntityInfo> entityInfos, QueryResultLoader resultLoader, int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("Incorrect value for fetchsize passed. Your fetchSize is less than 1");
      }

      this.entityInfos = entityInfos;
      this.resultLoader = resultLoader;
      this.fetchSize = fetchSize;

      // Set the values of first and max so that they can be used by the methods on the superclass.
      // Since this is the eager version, we know that we can set the 'first' field to 0.

      first = 0;

      // Similarly max can be set to the size of the list that gets passed in - 1. Using -1 because max is on base 0 while
      // the size of the list is base 1.

      max = entityInfos.size() - 1;

      buffer = new Object[this.fetchSize];
   }

   /**
    * Jumps to a given index in the list of results.
    *
    * @param index to jump to
    * @throws IndexOutOfBoundsException
    */
   @Override
   public void jumpToResult(int index) throws IndexOutOfBoundsException {
      if (index > entityInfos.size() || index < 0) {
         throw new IndexOutOfBoundsException("The index you entered is either greater than the size of the list or negative");
      }
      this.index = index;
   }

   @Override
   public void close() {
      // This method does not need to do anything for this type of iterator as when an instace of it is
      // created, the iterator() method in CacheQueryImpl closes everything that needs to be closed.
   }

   /**
    * Returns the next element in the list
    *
    * @return The next element in the list.
    */
   @Override
   public Object next() {
      if (!hasNext()) throw new IndexOutOfBoundsException("Out of boundaries. There is no next");

      Object toReturn;
      int bufferSize = buffer.length;

      // make sure the index we are after is in the buffer.  If it is, then index >= bufferIndex and index <= (bufferIndex + bufferSize).
      if (bufferIndex >= 0                                       // buffer init check
            && index >= bufferIndex                           // lower boundary
            && index < (bufferIndex + bufferSize))          // upper boundary
      {
         // now we can get this from the buffer.  Sweet!
         int indexToReturn = index - bufferIndex;
         toReturn = buffer[indexToReturn];
      } else {
         // We need to populate the buffer.

         toReturn = loadResult( index );

         //Wiping bufferObjects and the bufferIndex so that there is no stale data.

         Arrays.fill(buffer, null);
         buffer[0] = toReturn;

         // we now need to buffer item at index "index", as well as the next "fetchsize - 1" elements.  I.e., a total of fetchsize elements will be buffered.
         //now loop through bufferSize times to add the rest of the objects into the list.

         for (int i = 1; i < bufferSize; i++) {
            if (index + i > max) {
               log.debug("Your current index + bufferSize exceeds the size of your number of hits");
               break;
            }

            Object toBuffer = loadResult( index + i );
            buffer[i] = toBuffer;
         }
         bufferIndex = index;

      }

      index++;
      return toReturn;
   }

   private Object loadResult(int index) {
      return resultLoader.load(entityInfos.get( index ));
   }

   /**
    * Returns the previous element in the list.
    *
    * @return The previous element in the list.
    */
   @Override
   public Object previous() {
      if (!hasPrevious()) throw new IndexOutOfBoundsException("Index is out of bounds. There is no previous");

      Object toReturn;
      int bufferSize = buffer.length;

      // make sure the index we are after is in the buffer.  If it is, then index >= bufferIndex and index <= (bufferIndex + bufferSize).

      if (bufferIndex >= 0 // buffer init check
            && index <= bufferIndex // lower boundary
            && index >= (bufferIndex + bufferSize)) // upper boundary
      {
         // now we can get this from the buffer.  Sweet!
         int indexToReturn = bufferIndex - index;        // Unlike next() we have to make sure that we are subtracting index from bufferIndex
         toReturn = buffer[indexToReturn];
      } else {
         toReturn = loadResult( index );
         // Wiping bufferObjects and the bufferIndex so that there is no stale data.

         Arrays.fill(buffer, null);
         buffer[0] = toReturn;

         // we now need to buffer objects at index "index", as well as the next "fetchsize - 1" elements.
         // I.e., a total of fetchsize elements will be buffered.
         // now loop through bufferSize times to add the rest of the objects into the list.

         for (int i = 1; i < bufferSize; i++) {
            if (index - i < first) {
               log.debug("Your current index - bufferSize exceeds the size of your number of hits");
               break;
            }
            buffer[i] = loadResult( index - i );
         }
         bufferIndex = index;
      }
      index--;
      return toReturn;
   }

   /**
    * Returns the index of the element that would be returned by a subsequent call to next.
    *
    * @return Index of next element.
    */
   @Override
   public int nextIndex() {
      if (!hasNext()) throw new NoSuchElementException("Out of boundaries");
      return index + 1;
   }

   /**
    * Returns the index of the element that would be returned by a subsequent call to previous.
    *
    * @return Index of previous element.
    */
   @Override
   public int previousIndex() {
      if (!hasPrevious()) throw new NoSuchElementException("Out of boundaries");
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

}
