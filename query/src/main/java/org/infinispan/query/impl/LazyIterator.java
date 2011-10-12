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

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import net.jcip.annotations.NotThreadSafe;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.CacheException;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * Implementation for {@link org.infinispan.query.QueryIterator}. This is what is returned when the {@link
 * org.infinispan.query.CacheQuery#lazyIterator()} method is called. This loads the results only when required and hence
 * differs from {@link EagerIterator} which is the other implementation of QueryResultIterator.
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */
@NotThreadSafe
public class LazyIterator extends AbstractIterator {

   private final DocumentExtractor extractor;

   public LazyIterator(HSQuery hSearchQuery, AdvancedCache<?, ?> cache, int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("Incorrect value for fetchsize passed. Your fetchSize is less than 1");
      }
      this.extractor = hSearchQuery.queryDocumentExtractor(); //triggers actual Lucene search
      this.index = 0;
      this.max = hSearchQuery.queryResultSize() - 1;
      this.cache = cache;
      this.fetchSize = fetchSize;
      //Create an buffer with size fetchSize (which is the size of the required buffer).
      buffer = new Object[this.fetchSize];
   }

   public void jumpToResult(int index) throws IndexOutOfBoundsException {
      if (index < first || index > max) {
         throw new IndexOutOfBoundsException("The given index is incorrect. Please check and try again.");
      }
      this.index = first + index;
   }

   @Override
   public void close() {
      extractor.close();
   }

   public Object next() {
      if (!hasNext()) throw new IndexOutOfBoundsException("Index is out of bounds. There is no next");

      Object toReturn = null;
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
         // else we need to populate the buffer and get what we need.

         try {
            KeyTransformationHandler keyTransformationHandler = KeyTransformationHandler.getInstance(cache);

            String documentId = (String) extractor.extract(index).getId();
            toReturn = cache.get(keyTransformationHandler.stringToKey(documentId, cache.getClassLoader()));

            //Wiping bufferObjects and the bufferIndex so that there is no stale data.
            Arrays.fill(buffer, null);
            buffer[0] = toReturn;

            // we now need to buffer item at index "index", as well as the next "fetchsize - 1" elements.  I.e., a total of fetchsize elements will be buffered.
            // ignore loop below, in needs fixing
            //now loop through bufferSize times to add the rest of the objects into the list.

            for (int i = 1; i < bufferSize; i++) {
               String bufferDocumentId = (String) extractor.extract(index + i).getId();
               Object toBuffer = cache.get(keyTransformationHandler.stringToKey(bufferDocumentId, cache.getClassLoader()));
               buffer[i] = toBuffer;
            }
            bufferIndex = index;
         }
         catch (IOException e) {
            throw new CacheException();
         }
      }

      index++;
      return toReturn;
   }

   public Object previous() {
      if (!hasPrevious()) throw new IndexOutOfBoundsException("Index is out of bounds. There is no previous");

      Object toReturn = null;
      int bufferSize = buffer.length;

      // make sure the index we are after is in the buffer.  If it is, then index >= bufferIndex and index <= (bufferIndex + bufferSize).

      if (bufferIndex >= 0 // buffer init check
            && index <= bufferIndex // lower boundary
            && index >= (bufferIndex + bufferSize)) // upper boundary
      {
         // now we can get this from the buffer.  Sweet!
         int indexToReturn = bufferIndex - index;        // Unlike next() we have to make sure that we are subtracting index from bufferIndex
         toReturn = buffer[indexToReturn];
      }

      try {
         KeyTransformationHandler keyTransformationHandler = KeyTransformationHandler.getInstance(cache);

         //Wiping the buffer
         Arrays.fill(buffer, null);

         String documentId = (String) extractor.extract(index).getId();
         toReturn = cache.get(keyTransformationHandler.stringToKey(documentId, cache.getClassLoader()));

         buffer[0] = toReturn;

         //now loop through bufferSize times to add the rest of the objects into the list.
         for (int i = 1; i < bufferSize; i++) {
            String bufferDocumentId = (String) extractor.extract(index - i).getId();    //In this case it has to be index - i because previous() is called.
            Object toBuffer = cache.get(keyTransformationHandler.stringToKey(bufferDocumentId, cache.getClassLoader()));
            buffer[i] = toBuffer;
         }

         bufferIndex = index;
      }
      catch (IOException e) {
         e.printStackTrace();
      }
      index--;
      return toReturn;
   }

   public int nextIndex() {
      if (!hasNext()) throw new NoSuchElementException("Out of boundaries");
      return index + 1;
   }

   public int previousIndex() {
      if (!hasPrevious()) throw new NoSuchElementException("Out of boundaries.");
      return index - 1;
   }

   /**
    * This method is not supported and should not be used. Use cache.remove() instead.
    */
   public void remove() {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache");
   }

   /**
    * This method is not supported in and should not be called. Use cache.put() instead.
    *
    * @param o
    * @throws UnsupportedOperationException
    */
   public void set(Object o) throws UnsupportedOperationException {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache");
   }

   /**
    * This method is not supported in and should not be called. Use cache.put() instead.
    *
    * @param o
    * @throws UnsupportedOperationException
    */
   public void add(Object o) {
      throw new UnsupportedOperationException("Not supported as you are trying to change something in the cache");
   }

}
