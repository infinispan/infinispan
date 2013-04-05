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
package org.infinispan.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Similar to the JDK's AbstractMap, this provides common functionality for custom map implementations.  Unlike JDK's
 * AbstractMap, there is no support for null keys.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractMap<K, V> implements Map<K, V> {
   // views
   protected transient Set<Map.Entry<K, V>> entrySet = null;
   protected transient Set<K> keySet = null;
   protected transient Collection<V> values = null;

   @Override
   public int hashCode() {
      int h = 0;
      Iterator<Entry<K,V>> i = entrySet().iterator();
      while (i.hasNext())
         h += i.next().hashCode();
      return h;
   }

   // The normal bit spreader...
   protected static int hash(Object key) {
      int h = key.hashCode();
      h ^= (h >>> 20) ^ (h >>> 12);
      return h ^ (h >>> 7) ^ (h >>> 4);
   }

   protected static boolean eq(Object o1, Object o2) {
      return o1 == o2 || (o1 != null && o1.equals(o2));
   }

   protected static void assertKeyNotNull(Object key) {
      if (key == null) throw new NullPointerException("Null keys are not supported!");
   }

   protected static class SimpleEntry<K, V> implements Map.Entry<K, V> {
      private K key;
      private V value;

      SimpleEntry(K key, V value) {
         this.key = key;
         this.value = value;
      }

      SimpleEntry(Map.Entry<K, V> entry) {
         this.key = entry.getKey();
         this.value = entry.getValue();
      }

      @Override
      public K getKey() {
         return key;
      }

      @Override
      public V getValue() {
         return value;
      }

      @Override
      public V setValue(V value) {
         V old = this.value;
         this.value = value;
         return old;
      }

      public boolean equals(Object o) {
         if (this == o)
            return true;

         if (!(o instanceof Map.Entry))
            return false;
         Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
         return eq(key, e.getKey()) && eq(value, e.getValue());
      }

      public int hashCode() {
         return hash(key) ^
               (value == null ? 0 : hash(value));
      }

      public String toString() {
         return getKey() + "=" + getValue();
      }
   }
}
