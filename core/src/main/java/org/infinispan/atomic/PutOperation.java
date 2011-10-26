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
 * An atomic put operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
public class PutOperation<K, V> extends Operation<K, V> {
   private K key;
   private V oldValue;
   private V newValue;

   public PutOperation() {
   }

   PutOperation(K key, V oldValue, V newValue) {
      this.key = key;
      this.oldValue = oldValue;
      this.newValue = newValue;
   }

   public void rollback(Map<K, V> delegate) {
      if (oldValue == null)
         delegate.remove(key);
      else
         delegate.put(key, oldValue);
   }

   public void replay(Map<K, V> delegate) {
      delegate.put(key, newValue);
   }
   
   @Override
   public K keyAffected() {
      return key;
   }

   public static class Externalizer extends AbstractExternalizer<PutOperation> {
      @Override
      public void writeObject(ObjectOutput output, PutOperation put) throws IOException {
         output.writeObject(put.key);
         output.writeObject(put.newValue);
      }

      @Override
      public PutOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         PutOperation put = new PutOperation();
         put.key = input.readObject();
         put.newValue = input.readObject();         
         return put;
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_PUT_OPERATION;
      }

      @Override
      public Set<Class<? extends PutOperation>> getTypeClasses() {
         return Util.<Class<? extends PutOperation>>asSet(PutOperation.class);
      }
   }
}