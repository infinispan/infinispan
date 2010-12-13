/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshalls;

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

   public void rollback(Map<K, V> delegate) {
      if (oldValue != null) delegate.put(key, oldValue);
   }

   public void replay(Map<K, V> delegate) {
      delegate.remove(key);
   }

   @Marshalls(typeClasses = RemoveOperation.class, id = Ids.ATOMIC_REMOVE_OPERATION)
   public static class Externalizer implements org.infinispan.marshall.Externalizer<RemoveOperation> {
      public void writeObject(ObjectOutput output, RemoveOperation remove) throws IOException {
         output.writeObject(remove.key);
      }
      
      public RemoveOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RemoveOperation remove = new RemoveOperation();
         remove.key = input.readObject();
         return remove;
      }
   }
}