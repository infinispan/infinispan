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
package org.infinispan.distribution.ch;

import org.infinispan.CacheException;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates topology information from all nodes within the cluster.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TopologyInfo {
   
   private Map<Address, NodeTopologyInfo> address2TopologyInfo = new HashMap<Address, NodeTopologyInfo>();

   public TopologyInfo() {
   }

   public TopologyInfo(TopologyInfo topologyInfo) {
      this.address2TopologyInfo.putAll(topologyInfo.address2TopologyInfo);
   }

   public TopologyInfo(TopologyInfo topologyInfo, Collection<NodeTopologyInfo> mergeWith) {
      this(topologyInfo);
      for (NodeTopologyInfo nti: mergeWith)
         addNodeTopologyInfo(nti.getAddress(), nti);
   }

   public void addNodeTopologyInfo(Address addr, NodeTopologyInfo ti) {
      address2TopologyInfo.put(addr, ti);
   }

   public boolean isSameSite(Address a1, Address a2) {
      NodeTopologyInfo info1 = address2TopologyInfo.get(a1);
      if (info1 == null) throw new CacheException("No such address ( " + a1 + ") in the list of caches: " + address2TopologyInfo);
      NodeTopologyInfo info2 = address2TopologyInfo.get(a2);
      if (info2 == null) throw new CacheException("No such address ( " + a2 + ") in the list of caches: " + address2TopologyInfo);
      return info1.sameSite(info2);
   }

   public boolean isSameRack(Address a1, Address a2) {
      NodeTopologyInfo info1 = address2TopologyInfo.get(a1);
      NodeTopologyInfo info2 = address2TopologyInfo.get(a2);
      return info1.sameRack(info2);
   }

   public boolean isSameMachine(Address a1, Address a2) {
      NodeTopologyInfo info1 = address2TopologyInfo.get(a1);
      NodeTopologyInfo info2 = address2TopologyInfo.get(a2);
      return info1.sameMachine(info2);
   }

   public NodeTopologyInfo getNodeTopologyInfo(Address address) {
      return address2TopologyInfo.get(address);
   }

   public void removeNodeInfo(Address leaver) {
      address2TopologyInfo.remove(leaver);
   }

   public Collection<NodeTopologyInfo> getAllTopologyInfo() {
      return Collections.unmodifiableCollection(address2TopologyInfo.values());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TopologyInfo that = (TopologyInfo) o;

      if (address2TopologyInfo != null ? !address2TopologyInfo.equals(that.address2TopologyInfo) : that.address2TopologyInfo != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      return address2TopologyInfo != null ? address2TopologyInfo.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "TopologyInfo{" +
            "address2TopologyInfo=" + address2TopologyInfo +
            '}';
   }

   public boolean containsInfoForNode(Address address) {
      return address2TopologyInfo.get(address) != null;
   }
}
