/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.remoting.transport.jgroups;

import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.jgroups.util.TopologyUUID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * An encapsulation of a JGroups Address
 *
 * @author Bela Ban
 * @since 5.0
 */
public final class JGroupsTopologyAwareAddress extends JGroupsAddress implements TopologyAwareAddress {

   private final TopologyUUID topologyAddress;

   public JGroupsTopologyAwareAddress(TopologyUUID address) {
      super(address);
      topologyAddress = address;
   }


   @Override
   public String getSiteId() {
      return topologyAddress.getSiteId();
   }

   @Override
   public String getRackId() {
      return topologyAddress.getRackId();
   }

   @Override
   public String getMachineId() {
      return topologyAddress.getMachineId();
   }


   @Override
   public boolean isSameSite(TopologyAwareAddress addr) {
      return getSiteId() == null ? addr.getSiteId() == null : getSiteId().equals(addr.getSiteId());
   }

   @Override
   public boolean isSameRack(TopologyAwareAddress addr) {
      if (!isSameSite(addr))
         return false;
      return getRackId() == null ? addr.getRackId() == null : getRackId().equals(addr.getRackId());
   }

   @Override
   public boolean isSameMachine(TopologyAwareAddress addr) {
      if (!isSameRack(addr))
         return false;
      return getMachineId() == null ? addr.getMachineId() == null : getMachineId().equals(addr.getMachineId());
   }

   public static final class Externalizer implements org.infinispan.marshall.AdvancedExternalizer<JGroupsTopologyAwareAddress> {
      @Override
      public void writeObject(ObjectOutput output, JGroupsTopologyAwareAddress address) throws IOException {
         try {
            org.jgroups.util.Util.writeAddress(address.address, output);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public JGroupsTopologyAwareAddress readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         try {
            TopologyUUID jgroupsAddress = (TopologyUUID) org.jgroups.util.Util.readAddress(unmarshaller);
            return new JGroupsTopologyAwareAddress(jgroupsAddress);
         } catch (Exception e) {
            throw new IOException(e);
         }
      }

      @Override
      public Set<Class<? extends JGroupsTopologyAwareAddress>> getTypeClasses() {
         return Collections.<Class<? extends JGroupsTopologyAwareAddress>>singleton(JGroupsTopologyAwareAddress.class);
      }

      @Override
      public Integer getId() {
         return Ids.JGROUPS_TOPOLOGY_AWARE_ADDRESS;
      }
   }
}
