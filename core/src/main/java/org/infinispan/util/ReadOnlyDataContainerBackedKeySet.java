/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.container.DataContainer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * A Set view of keys in a data container, which is read-only and has efficient contains(), unlike some data container
 * ley sets.
 *
 * @author Manik Surtani
 * @since 4.1
 */
public class ReadOnlyDataContainerBackedKeySet implements Set<Object> {

   final DataContainer container;
   Set<Object> keySet;

   public ReadOnlyDataContainerBackedKeySet(DataContainer container) {
      this.container = container;
   }

   @Override
   public int size() {
      return container.size();
   }

   @Override
   public boolean isEmpty() {
      return container.size() == 0;
   }

   @Override
   public boolean contains(Object o) {
      return container.containsKey(o);
   }

   @Override
   public Iterator<Object> iterator() {
      if (keySet == null) keySet = container.keySet();
      return keySet.iterator();
   }

   @Override
   public Object[] toArray() {
      if (keySet == null) keySet = container.keySet();
      return keySet.toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      if (keySet == null) keySet = container.keySet();
      return keySet.toArray(a);
   }

   @Override
   public boolean add(Object o) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      boolean ca = true;
      for (Object o: c) {
         ca = ca && contains(o);
         if (!ca) return false;
      }
      return ca;
   }

   @Override
   public boolean addAll(Collection<?> c) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("Immutable");
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException("Immutable");
   }
}
