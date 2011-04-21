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

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Holds topology information about a a node.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class NodeTopologyInfo {

   private final String machineId;
   private final String rackId;
   private final String siteId;
   private final Address address;

   public NodeTopologyInfo(String machineId, String rackId, String siteId, Address address) {
      this.machineId = machineId;
      this.rackId = rackId;
      this.siteId = siteId;
      this.address = address;
   }

   public String getMachineId() {
      return machineId;
   }

   public String getRackId() {
      return rackId;
   }

   public String getSiteId() {
      return siteId;
   }

   public boolean sameSite(NodeTopologyInfo info2) {
      return equalObjects(siteId, info2.siteId);
   }

   public boolean sameRack(NodeTopologyInfo info2) {
      return sameSite(info2) && equalObjects(rackId, info2.rackId);
   }

   public boolean sameMachine(NodeTopologyInfo info2) {
      return sameRack(info2) && equalObjects(machineId, info2.machineId);
   }

   private boolean equalObjects(Object first, Object second) {
      return first == null ? second == null : first.equals(second);
   }

   public Address getAddress() {
      return address;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NodeTopologyInfo that = (NodeTopologyInfo) o;

      if (address != null ? !address.equals(that.address) : that.address != null) return false;
      if (machineId != null ? !machineId.equals(that.machineId) : that.machineId != null) return false;
      if (rackId != null ? !rackId.equals(that.rackId) : that.rackId != null) return false;
      if (siteId != null ? !siteId.equals(that.siteId) : that.siteId != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = machineId != null ? machineId.hashCode() : 0;
      result = 31 * result + (rackId != null ? rackId.hashCode() : 0);
      result = 31 * result + (siteId != null ? siteId.hashCode() : 0);
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "NodeTopologyInfo{" +
            "machineId='" + machineId + '\'' +
            ", rackId='" + rackId + '\'' +
            ", siteId='" + siteId + '\'' +
            ", address=" + address +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<NodeTopologyInfo> {
      @Override
      public void writeObject(ObjectOutput output, NodeTopologyInfo nti) throws IOException {
         output.writeObject(nti.siteId);
         output.writeObject(nti.rackId);
         output.writeObject(nti.machineId);
         output.writeObject(nti.address);
      }

      @Override
      public NodeTopologyInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String siteId = (String) input.readObject();
         String rackId = (String) input.readObject();
         String machineId = (String) input.readObject();
         Address address = (Address) input.readObject();
         return new NodeTopologyInfo(machineId, rackId, siteId, address);
      }

      @Override
      public Integer getId() {
         return Ids.NODE_TOPOLOGY_INFO;
      }

      @Override
      public Set<Class<? extends NodeTopologyInfo>> getTypeClasses() {
         return Util.<Class<? extends NodeTopologyInfo>>asSet(NodeTopologyInfo.class);
      }
   }
}
