/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.server.memcached;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Value.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
class Value implements Externalizable {
   private int flags;
   private byte[] data;
   
   Value(int flags, byte[] data) {
      this.flags = flags;
      this.data = data;
   }

   public int getFlags() {
      return flags;
   }

   public byte[] getData() {
      return data;
   }

   @Override
   public boolean equals(Object obj) {
      if (obj == this)
         return true;
      if (!(obj instanceof Value))
         return false;
      Value other = (Value) obj;
      return Arrays.equals(data, other.data) && flags == other.flags;
   }

   @Override
   public int hashCode() {
      int result = 17;
      result = 31 * result + flags;
      result = 31 * result + data.hashCode();
      return result;
   }

   @Override
   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      flags = in.read();
      data = new byte[in.read()];
      in.read(data);
   }

   @Override
   public void writeExternal(ObjectOutput out) throws IOException {
      out.write(flags);
      out.write(data.length);
      out.write(data);
   }
}
