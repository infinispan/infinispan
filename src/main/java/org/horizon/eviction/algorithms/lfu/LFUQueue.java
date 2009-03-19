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
package org.horizon.eviction.algorithms.lfu;

import org.horizon.eviction.algorithms.BaseEvictionQueue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Least frequently used queue for use with the {@link org.horizon.eviction.algorithms.lfu.LFUAlgorithm}.  This
 * implementation guarantees O (ln n) for put and remove, visit an iteration operations.
 * <p/>
 * // TODO investigate using a Fibonacci Heap or a Brodal Queue (http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.43.8133)
 * // another potential is Bernard Chazelle's SoftHeap
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class LFUQueue extends BaseEvictionQueue {
   Map<Object, Integer> visitLog = new HashMap<Object, Integer>();
   Map<Integer, Set<Object>> iterationMap = new TreeMap<Integer, Set<Object>>(new Comparator<Integer>() {
      public int compare(Integer o1, Integer o2) {
         return (o1 < o2 ? 1 : (o1 == o2 ? 0 : -1));
      }
   });

   final Set<Object> getKeySet(int numVisits) {
      Set<Object> set = iterationMap.get(numVisits);
      if (set == null) {
         set = new HashSet<Object>();
         iterationMap.put(numVisits, set);
      }
      return set;
   }

   final void addToKeySet(Object key, int numVisits) {
      getKeySet(numVisits).add(key);
   }

   final void removeFromKeySet(Object key, int numVisits) {
      Set<Object> set = iterationMap.get(numVisits);
      if (set != null) {
         set.remove(key);
         if (set.isEmpty()) iterationMap.remove(numVisits);
      }
   }

   public void visit(Object key) {
      Integer numVisits = visitLog.get(key);
      if (numVisits != null) {
         removeFromKeySet(key, numVisits);
         numVisits++;
         addToKeySet(key, numVisits);
         visitLog.put(key, numVisits);
      }
   }

   public boolean contains(Object key) {
      return visitLog.containsKey(key);
   }

   public void remove(Object key) {
      Integer numVisits = visitLog.remove(key);
      if (numVisits != null) {
         removeFromKeySet(key, numVisits);
      }
   }

   public void add(Object key) {
      if (!visitLog.containsKey(key)) {
         // new, so numVisits is 1.
         getKeySet(1).add(key);
         visitLog.put(key, 1);
      }
   }

   public int size() {
      return visitLog.size();
   }

   @Override
   public boolean isEmpty() {
      return visitLog.size() == 0;
   }

   public void clear() {
      visitLog.clear();
      visitLog = new HashMap<Object, Integer>();
      iterationMap.clear();
      iterationMap = new HashMap<Integer, Set<Object>>();
   }

   public Iterator<Object> iterator() {
      return new Iterator<Object>() {
         Iterator<Set<Object>> setIterator = iterationMap.values().iterator();
         Set<Object> currentKeySet;
         Iterator<Object> keyIterator;
         Object currentKey;

         public boolean hasNext() {
            while (keyIterator == null || !keyIterator.hasNext()) {
               if (setIterator.hasNext()) {
                  currentKeySet = setIterator.next();
                  keyIterator = currentKeySet.iterator();
               } else return false;
            }
            return true;
         }

         public Object next() {
            if (keyIterator == null && !hasNext()) throw new IllegalStateException("Out of bounds");
            return (currentKey = keyIterator.next());
         }

         public void remove() {
            if (currentKey == null) throw new IllegalStateException("Call next() before remove()!");
            keyIterator.remove();
            if (currentKeySet.isEmpty()) setIterator.remove();
            visitLog.remove(currentKey);
         }
      };
   }
}

