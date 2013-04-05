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
package org.infinispan.util.concurrent;

import org.infinispan.util.CollectionFactory;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple Set implementation backed by a {@link java.util.concurrent.ConcurrentHashMap} to deal with the fact that the
 * JDK does not have a proper concurrent Set implementation that uses efficient lock striping.
 * <p/>
 * Note that values are stored as keys in the underlying Map, with a static dummy object as value.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */
public class ConcurrentHashSet<E> extends AbstractSet<E> implements Serializable {
   
   /** The serialVersionUID */
   private static final long serialVersionUID = 5312604953511379869L;
   
   protected final ConcurrentMap<E, Object> map;
   
   /** any Serializable object will do, Integer.valueOf(0) is known cheap **/
   private static final Serializable DUMMY = 0;

   public ConcurrentHashSet() {
      map = CollectionFactory.makeConcurrentMap();
   }

   /**
    * @param concurrencyLevel passed in to the underlying CHM.  See {@link java.util.concurrent.ConcurrentHashMap#ConcurrentHashMap(int,
    *                         float, int)} javadocs for details.
    */
   public ConcurrentHashSet(int concurrencyLevel) {
      map = CollectionFactory.makeConcurrentMap(16, concurrencyLevel);
   }

   /**
    * Params passed in to the underlying CHM.  See {@link java.util.concurrent.ConcurrentHashMap#ConcurrentHashMap(int,
    * float, int)} javadocs for details.
    */
   public ConcurrentHashSet(int initSize, float loadFactor, int concurrencyLevel) {
      map = CollectionFactory.makeConcurrentMap(initSize, loadFactor, concurrencyLevel);
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
   public Iterator<E> iterator() {
      return map.keySet().iterator();
   }

   @Override
   public Object[] toArray() {
      return map.keySet().toArray();
   }

   @Override
   public <T> T[] toArray(T[] a) {
      return map.keySet().toArray(a);
   }

   @Override
   public boolean add(E o) {
      Object v = map.put(o, DUMMY);
      return v == null;
   }

   @Override
   public boolean remove(Object o) {
      Object v = map.remove(o);
      return v != null;
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      return map.keySet().containsAll(c);
   }

   @Override
   public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException("Not supported in this implementation since additional locking is required and cannot directly be delegated to multiple calls to ConcurrentHashMap");
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not supported in this implementation since additional locking is required and cannot directly be delegated to multiple calls to ConcurrentHashMap");
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not supported in this implementation since additional locking is required and cannot directly be delegated to multiple calls to ConcurrentHashMap");
   }

   @Override
   public void clear() {
      map.clear();
   }
}
