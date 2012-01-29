/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.global;

import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.TypedProperties;

public class TransportConfiguration {

   private final String clusterName;
   private final String machineId;
   private final String rackId;
   private final String siteId;
   private final boolean strictPeerToPeer;
   private final long distributedSyncTimeout;
   private final Transport transport;
   private final String nodeName;
   private TypedProperties properties;
   
   TransportConfiguration(String clusterName, String machineId, String rackId, String siteId, boolean strictPeerToPeer,
         long distributedSyncTimeout, Transport transport, String nodeName, TypedProperties properties) {
      this.clusterName = clusterName;
      this.machineId = machineId;
      this.rackId = rackId;
      this.siteId = siteId;
      this.strictPeerToPeer = strictPeerToPeer;
      this.distributedSyncTimeout = distributedSyncTimeout;
      this.transport = transport;
      this.nodeName = nodeName;
      this.properties = properties;
   }

   public String clusterName() {
      return clusterName;
   }

   public String machineId() {
      return machineId;
   }

   public String rackId() {
      return rackId;
   }

   public String siteId() {
      return siteId;
   }

   public long distributedSyncTimeout() {
      return distributedSyncTimeout;
   }
   
   public Transport transport() {
      return transport;
   }

   public String nodeName() {
      return nodeName;
   }

   public boolean strictPeerToPeer() {
      return strictPeerToPeer;
   }
   
   public TypedProperties properties() {
      return properties;
   }
   
   public boolean hasTopologyInfo() {
      return siteId() != null || rackId() != null || machineId() != null;
   }
}