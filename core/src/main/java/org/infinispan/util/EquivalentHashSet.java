/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.util;

import java.util.AbstractSet;
import java.util.Iterator;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public class EquivalentHashSet<E> extends AbstractSet<E> {

   // Dummy value to associate with an Object in the backing Map
   private static final Object PRESENT = new Object();

   private final Equivalence<E> entryEq;

   private final EquivalentHashMap<E,Object> map;

   public EquivalentHashSet(Equivalence<E> entryEq) {
      this.entryEq = entryEq;
      map = new EquivalentHashMap<E, Object>(
            entryEq, AnyEquivalence.OBJECT);
   }

   public EquivalentHashSet(int initialCapacity, Equivalence<E> entryEq) {
      this.entryEq = entryEq;
      map = new EquivalentHashMap<E, Object>(
            initialCapacity, entryEq, AnyEquivalence.OBJECT);
   }

   @Override
   public Iterator<E> iterator() {
      return map.keySet().iterator();
   }

   @Override
   public int size() {
      return map.size();
   }

   @Override
   public boolean isEmpty() {
      return map.isEmpty();
   }

   @Override
   public boolean contains(Object o) {
      return map.containsKey(o);
   }

   @Override
   public boolean add(E e) {
      return map.put(e, PRESENT) == null;
   }

   @Override
   public boolean remove(Object o) {
      return map.remove(o) == PRESENT;
   }

   @Override
   public void clear() {
      map.clear();
   }

   @Override
   public int hashCode() {
      int h = 0;
      Iterator<E> i = iterator();
      while (i.hasNext()) {
         E obj = i.next();
         if (obj != null)
            h += entryEq.hashCode(obj);
      }
      return h;
   }

}
