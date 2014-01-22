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
package org.infinispan.atomic;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

/**
 * An atomic remove operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
public class RemoveOperation<K, V> extends Operation<K, V> {
   private K key;
   private V oldValue;

   public RemoveOperation() {
   }

   RemoveOperation(K key, V oldValue) {
      this.key = key;
      this.oldValue = oldValue;
   }

   @Override
   public void rollback(Map<K, V> delegate) {
      if (oldValue != null) delegate.put(key, oldValue);
   }

   @Override
   public void replay(Map<K, V> delegate) {
      delegate.remove(key);
   }
   
   @Override
   public K keyAffected() {
      return key;
   }

   @Override
   public String toString() {
      return "RemoveOperation{" +
            "key=" + key +
            ", oldValue=" + oldValue +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<RemoveOperation> {
      @Override
      public void writeObject(ObjectOutput output, RemoveOperation remove) throws IOException {
         output.writeObject(remove.key);
      }

      @Override
      public RemoveOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RemoveOperation<Object, Object> remove = new RemoveOperation<Object, Object>();
         remove.key = input.readObject();
         return remove;
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_REMOVE_OPERATION;
      }

      @Override
      public Set<Class<? extends RemoveOperation>> getTypeClasses() {
         return Util.<Class<? extends RemoveOperation>>asSet(RemoveOperation.class);
      }
   }
}