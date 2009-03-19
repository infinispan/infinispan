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
package org.horizon.eviction.algorithms.nullalgo;

import org.horizon.eviction.algorithms.BaseEvictionQueue;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A queue that does nothing.
 *
 * @author Brian Stansberry
 * @since 4.0
 */
public class NullEvictionQueue extends BaseEvictionQueue {
   /**
    * Singleton instance of this class.
    */
   public static final NullEvictionQueue INSTANCE = new NullEvictionQueue();

   /**
    * Constructs a new NullEvictionQueue.
    */
   private NullEvictionQueue() {
   }

   /**
    * No-op
    */
   public void add(Object key) {
      // no-op
   }

   /**
    * No-op
    */
   public void clear() {
      // no-op
   }

   /**
    * Returns <code>false</code>
    */
   public boolean contains(Object key) {
      return false;
   }

   /**
    * Returns <code>0</code>
    */
   public int size() {
      return 0;
   }

   /**
    * Returns an <code>Iterator</code> whose <code>hasNext()</code> returns <code>false</code>.
    */
   public Iterator<Object> iterator() {
      return NullQueueIterator.INSTANCE;
   }

   /**
    * No-op
    */
   public void remove(Object key) {
      // no-op
   }

   static class NullQueueIterator implements Iterator<Object> {
      private static final NullQueueIterator INSTANCE = new NullQueueIterator();

      private NullQueueIterator() {
      }

      public boolean hasNext() {
         return false;
      }

      public Object next() {
         throw new NoSuchElementException("No more elements");
      }

      public void remove() {
         throw new IllegalStateException("Must call next() before remove()");
      }
   }

}
