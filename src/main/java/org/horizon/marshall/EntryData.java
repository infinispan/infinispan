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
package org.horizon.marshall;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Serializable representation of an entry in the cache
 *
 * @author Bela Ban
 * @since 1.0
 */
public class EntryData<K, V> implements Externalizable, Map.Entry<K, V> {
   private K key;
   private V value;

   static final long serialVersionUID = -7571995794010294485L;

   public EntryData(K key, V value) {
      this.key = key;
      this.value = value;
   }

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }

   // TODO: Remove and replace with methods in the CacheMarshaller so that we can use the same marshalling framework
   public void writeExternal(ObjectOutput out) throws IOException {
      out.writeObject(key);
      out.writeObject(value);
   }

   // TODO: Remove in and replace with methods in the CacheMarshaller so that we can use the same marshalling framework
   @SuppressWarnings("unchecked")
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      key = (K) in.readObject();
      value = (V) in.readObject();
   }

   @Override
   public String toString() {
      return "{" + key + "=" + value + "}";
   }

   private static boolean eq(Object a, Object b) {
      return a == b || (a != null && a.equals(b));
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof EntryData))
         return false;

      EntryData<?, ?> other = (EntryData<?, ?>) o;
      return eq(key, other.key) && eq(value, other.value);
   }

   @Override
   public int hashCode() {
      int result;
      result = (key != null ? key.hashCode() : 0);
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }

   public V setValue(V value) {
      throw new UnsupportedOperationException();
   }
}
