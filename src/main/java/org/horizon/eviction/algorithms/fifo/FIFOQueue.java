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
package org.horizon.eviction.algorithms.fifo;

import org.horizon.eviction.algorithms.BaseEvictionQueue;

import java.util.Iterator;
import java.util.LinkedHashSet;

/**
 * First in, first out eviction queue implementation for the {@link FIFOAlgorithm}.  Guarantees O(1) for put, remove,
 * and the iterator.  Ignores {@link org.horizon.eviction.EvictionQueue#visit(Object)} as it doesn't order on visitor
 * count.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class FIFOQueue extends BaseEvictionQueue {
   private LinkedHashSet<Object> orderedKeys;

   protected FIFOQueue() {
      orderedKeys = new LinkedHashSet<Object>();
      // We use a LinkedHashSet here because we want to maintain FIFO ordering and still get the benefits of
      // O(1) for put/remove/iterate
   }

   public boolean contains(Object key) {
      return orderedKeys.contains(key);
   }

   public void remove(Object key) {
      orderedKeys.remove(key);
   }

   public void add(Object key) {
      if (!orderedKeys.contains(key)) orderedKeys.add(key);
   }

   public int size() {
      return orderedKeys.size();
   }

   public void clear() {
      orderedKeys.clear();
   }

   public Iterator<Object> iterator() {
      return orderedKeys.iterator();
   }
}
