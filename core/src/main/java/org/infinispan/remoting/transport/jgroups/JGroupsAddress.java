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
package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class JGroupsAddress implements Address {

   protected final org.jgroups.Address address;
   private final int hashCode;

   public JGroupsAddress(final org.jgroups.Address address) {
      if (address == null)
         throw new IllegalArgumentException("Address shall not be null");
      this.address = address;
      this.hashCode = address.hashCode();
   }

   @Override
   public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JGroupsAddress that = (JGroupsAddress) o;

      return address.equals(that.address);
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public String toString() {
      return String.valueOf(address);
   }

   public org.jgroups.Address getJGroupsAddress() {
      return address;
   }

   @Override
   public int compareTo(Address o) {
      JGroupsAddress oa = (JGroupsAddress) o;
      return address.compareTo(oa.address);
   }

   public static final class Externalizer extends AbstractExternalizer<JGroupsAddress> {
      @Override
      public void writeObject(ObjectOutput output, JGroupsAddress address) throws IOException {
         try {
            org.jgroups.util.Util.writeAddress(address.address, output);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public JGroupsAddress readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         try {
            org.jgroups.Address address = org.jgroups.util.Util.readAddress(unmarshaller);
            return new JGroupsAddress(address);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public Integer getId() {
         return Ids.JGROUPS_ADDRESS;
      }

      @Override
      public Set<Class<? extends JGroupsAddress>> getTypeClasses() {
         return Util.<Class<? extends JGroupsAddress>>asSet(JGroupsAddress.class);
      }
   }
}
