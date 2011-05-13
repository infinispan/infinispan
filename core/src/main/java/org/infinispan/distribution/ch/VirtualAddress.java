/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.ch;

import static org.infinispan.util.Util.formatString;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

/**
 * Virtual addresses are used internally by the consistent hashes in order to provide virtual nodes.
 * 
 * A virtual addresses contains the "real address" of the node and a synthetic identifier which
 * is used to differentiate different virtual nodes on a real node from one another.
 * 
 * @author Pete Muir
 *
 */
public class VirtualAddress implements Address {
   
   private final Address realAddress;
   private final int id;
   
   public VirtualAddress(Address realAddress, int id) {
      if (realAddress == null)
         throw new IllegalArgumentException(formatString("readAddress must not be null"));
      this.realAddress = realAddress;
      this.id = id;
   }
   
   public int getId() {
      return id;
   }
   
   public Address getRealAddress() {
      return realAddress;
   }
   
   @Override
   public int hashCode() {
      int result = realAddress.hashCode();
      result = 31 * result + id;
      return result;
   }
   
   @Override
   public boolean equals(Object obj) {
      if (obj instanceof VirtualAddress) {
         VirtualAddress that = (VirtualAddress) obj;
         return this.realAddress.equals(that.realAddress) && this.id == that.id;
      } else 
         return false;
   }
   
   @Override
   public String toString() {
      return formatString("%s-%d", realAddress, id);
   }
   
   public static class Externalizer extends AbstractExternalizer<VirtualAddress> {
      @Override
      public void writeObject(ObjectOutput output, VirtualAddress address) throws IOException {
         output.writeObject(address.realAddress);
         output.writeInt(address.id);
      }

      @Override
      public VirtualAddress readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         Address realAddress = (Address) unmarshaller.readObject();
         int id = unmarshaller.readInt();
         VirtualAddress address = new VirtualAddress(realAddress, id);
         return address;
      }

      @Override
      public Integer getId() {
         return Ids.VIRTUAL_ADDRESS;
      }

      @Override
      public Set<Class<? extends VirtualAddress>> getTypeClasses() {
         return Util.<Class<? extends VirtualAddress>>asSet(VirtualAddress.class);
      }
   }
   
}