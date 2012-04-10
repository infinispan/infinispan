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

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Where is Java 1.6?
 *
 * @author (various)
 * @since 4.0
 */
public class SimpleImmutableEntry<K, V> implements Map.Entry<K, V>, Serializable {
   private static final long serialVersionUID = -6092752114794052323L;

   private final K key;

   private final V value;

   public SimpleImmutableEntry(Entry<K, V> me) {
      key = me.getKey();
      value = me.getValue();
   }

   public SimpleImmutableEntry(K key, V value) {
      this.key = key;
      this.value = value;
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
   public V setValue(V arg0) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean equals(Object o) {
      if (!(o instanceof Map.Entry))
         return false;
      Map.Entry<?, ?> e2 = (Map.Entry<?, ?>) o;
      return (getKey() == null ? e2.getKey() == null : getKey().equals(e2.getKey()))
            && (getValue() == null ? e2.getValue() == null : getValue().equals(e2.getValue()));
   }

   @Override
   public int hashCode() {
      return (getKey() == null ? 0 : getKey().hashCode()) ^
            (getValue() == null ? 0 : getValue().hashCode());
   }

   @Override
   public String toString() {
      return key + "=" + value;
   }
}