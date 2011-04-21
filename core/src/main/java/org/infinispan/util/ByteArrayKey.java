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

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Wrapper class for byte[] keys.
 *
 * The class can be marshalled either via its externalizer or via the JVM
 * serialization.  The reason for supporting both methods is to enable
 * third-party libraries to be able to marshall/unmarshall them using standard
 * JVM serialization rules.  The Infinispan marshalling layer will always
 * chose the most performant one, aka the Externalizer method.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Marshallable(externalizer = ByteArrayKey.Externalizer.class, id = Ids.BYTE_ARRAY_KEY)
public class ByteArrayKey implements Serializable {

   private final byte[] data;

   public ByteArrayKey(byte[] data) {
      this.data = data;
   }

   public byte[] getData() {
      return data;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      ByteArrayKey key = (ByteArrayKey) obj;
      return Arrays.equals(key.data, this.data);
   }

   @Override
   public int hashCode() {
      return 41 + Arrays.hashCode(data);
   }

   @Override
   public String toString() {
      return new StringBuilder().append("ByteArrayKey").append("{")
         .append("data=").append(Util.printArray(data, true))
         .append("}").toString();
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object object) throws IOException {
         ByteArrayKey key = (ByteArrayKey) object;
         output.writeInt(key.data.length);
         output.write(key.data);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte[] data = new byte[input.readInt()];
         input.readFully(data);
         return new ByteArrayKey(data);
      }
   }
   
}
