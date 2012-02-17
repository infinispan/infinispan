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

   @Override
   public String toString() {
      return "TransportConfiguration{" +
            "clusterName='" + clusterName + '\'' +
            ", machineId='" + machineId + '\'' +
            ", rackId='" + rackId + '\'' +
            ", siteId='" + siteId + '\'' +
            ", strictPeerToPeer=" + strictPeerToPeer +
            ", distributedSyncTimeout=" + distributedSyncTimeout +
            ", transport=" + transport +
            ", nodeName='" + nodeName + '\'' +
            ", properties=" + properties +
            '}';
   }

}