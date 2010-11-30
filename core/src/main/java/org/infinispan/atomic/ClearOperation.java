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

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.util.FastCopyHashMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * An atomic clear operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
@Marshallable(externalizer = ClearOperation.Externalizer.class, id = Ids.ATOMIC_CLEAR_OPERATION)
public class ClearOperation<K, V> extends Operation<K, V> {
   FastCopyHashMap<K, V> originalEntries;

   ClearOperation() {
   }

   ClearOperation(FastCopyHashMap<K, V> originalEntries) {
      this.originalEntries = originalEntries;
   }

   public void rollback(Map<K, V> delegate) {
      if (!originalEntries.isEmpty()) delegate.putAll(originalEntries);
   }

   public void replay(Map<K, V> delegate) {
      delegate.clear();
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         // no-op
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClearOperation();
      }
   }
}