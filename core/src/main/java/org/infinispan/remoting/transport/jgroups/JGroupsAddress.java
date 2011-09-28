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
   org.jgroups.Address address;

   public JGroupsAddress() {
   }

   public JGroupsAddress(org.jgroups.Address address) {
      this.address = address;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JGroupsAddress that = (JGroupsAddress) o;

      if (address != null ? !address.equals(that.address) : that.address != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return address != null ? address.hashCode() : 0;
   }

   @Override
   public String toString() {
      return String.valueOf(address);
   }

   public org.jgroups.Address getJGroupsAddress() {
      return address;
   }

   public void setJGroupsAddress(org.jgroups.Address address) {
      this.address = address;
   }

   public static class Externalizer extends AbstractExternalizer<JGroupsAddress> {
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
         JGroupsAddress address = new JGroupsAddress();
         try {
            address.address = org.jgroups.util.Util.readAddress(unmarshaller);
            return address;
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
