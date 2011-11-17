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

   public String getClusterName() {
      return clusterName;
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

   public long getDistributedSyncTimeout() {
      return distributedSyncTimeout;
   }
   
   public Transport getTransport() {
      return transport;
   }

   public String getNodeName() {
      return nodeName;
   }

   public boolean isStrictPeerToPeer() {
      return strictPeerToPeer;
   }
   
   public TypedProperties getProperties() {
      return properties;
   }
   
   public boolean hasTopologyInfo() {
      return getSiteId() != null || getRackId() != null || getMachineId() != null;
   }
}