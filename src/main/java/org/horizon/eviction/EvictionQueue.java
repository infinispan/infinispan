/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.eviction;

/**
 * The eviction queue interface defines a contract for the queue implementations used by {@link EvictionAlgorithm}
 * implementations.  Queues are meant to be sorted in order of preference of keys for eviction, such that by using the
 * iterator exposed by the queue, the first element would be the most preferable to evict.
 * <p/>
 * The iterator provided should support {@link java.util.Iterator#remove()}.
 * <p/>
 * Note that there is no requirement that implementations should be thread safe, as the {@link
 * org.horizon.eviction.EvictionManager} would guarantee that only a single thread ever acccess the eviction queue at
 * any given time.
 * <p/>
 * Note that this is not to be confused with a JDK {@link java.util.Queue}, as it has no bearing or relationship to one,
 * although implementations may choose to use a JDK {@link java.util.Queue} as an underlying data structure.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface EvictionQueue extends Iterable<Object> {

   /**
    * Tests whether queue is empty
    *
    * @return true if the queue is empty; false otherwise
    */
   boolean isEmpty();

   /**
    * Informs the queue that an entry has been visited.  Implementations may choose to ignore this invocation, or use it
    * to update internal ordering based on the type of queue.
    *
    * @param key key visited
    */
   void visit(Object key);


   /**
    * Check if eviction entry data exists in the queue
    *
    * @param key to check for
    * @return true if the entry exists, false otherwise
    */
   boolean contains(Object key);

   /**
    * Remove eviction entry data from the queue.  A no-op if the specified object does not exist.
    *
    * @param key to remove
    */
   void remove(Object key);

   /**
    * Add entry eviction data to the queue
    *
    * @param key key to add.  Must not be null.
    */
   void add(Object key);

   /**
    * Get the size of the queue
    *
    * @return the size of the queue
    */
   int size();

   /**
    * Clear the queue.
    */
   void clear();
}
