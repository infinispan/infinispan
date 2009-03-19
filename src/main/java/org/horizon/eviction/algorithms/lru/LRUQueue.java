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
package org.horizon.eviction.algorithms.lru;

import org.horizon.eviction.algorithms.BaseEvictionQueue;
import org.horizon.util.VisitableBidirectionalLinkedHashSet;

import java.util.Iterator;

/**
 * Least Recently Used eviction queue implementation for the {@link LRUAlgorithm}.  Guarantees O(1) for put, remove, and
 * the iterator.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class LRUQueue extends BaseEvictionQueue {

   protected VisitableBidirectionalLinkedHashSet<Object> keys = new VisitableBidirectionalLinkedHashSet<Object>(true);

   @Override
   public void visit(Object key) {
      keys.visit(key);
   }

   public boolean contains(Object key) {
      return keys.contains(key);
   }

   public void remove(Object key) {
      keys.remove(key);
   }

   public void add(Object key) {
      if (keys.contains(key))
         keys.visit(key);
      else
         keys.add(key);
   }

   public int size() {
      return keys.size();
   }

   public void clear() {
      keys.clear();
   }

   public Iterator<Object> iterator() {
      return keys.iterator();
   }

   @Override
   public String toString() {
      return keys.toString();
   }
}
