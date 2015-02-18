package org.infinispan.configuration.global;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeInitializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.remoting.transport.Transport;

public class TransportConfiguration {
   static final AttributeDefinition<String> CLUSTER_NAME = AttributeDefinition.builder("clusterName", "ISPN")
         .immutable().build();
   static final AttributeDefinition<String> MACHINE_ID = AttributeDefinition.builder("machineId", null, String.class)
         .immutable().build();
   static final AttributeDefinition<String> RACK_ID = AttributeDefinition.builder("rackId", null, String.class)
         .immutable().build();
   static final AttributeDefinition<String> SITE_ID = AttributeDefinition.builder("siteId", null, String.class)
         .immutable().build();
   static final AttributeDefinition<String> NODE_NAME = AttributeDefinition.builder("nodeName", null, String.class)
         .immutable().build();
   static final AttributeDefinition<Long> DISTRIBUTED_SYNC_TIMEOUT = AttributeDefinition.builder(
         "distributedSyncTimeout", TimeUnit.MINUTES.toMillis(4)).build();
   static final AttributeDefinition<Transport> TRANSPORT = AttributeDefinition
         .builder("transport", null, Transport.class).immutable().build();
   static final AttributeDefinition<TypedProperties> PROPERTIES = AttributeDefinition
         .builder("properties", null, TypedProperties.class).initializer(new AttributeInitializer<TypedProperties>() {
            @Override
            public TypedProperties initialize() {
               return new TypedProperties();
            }
         }).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TransportConfiguration.class, CLUSTER_NAME, MACHINE_ID, RACK_ID, SITE_ID, NODE_NAME,
            DISTRIBUTED_SYNC_TIMEOUT, TRANSPORT, PROPERTIES);
   }

   private final AttributeSet attributes;
   private final ThreadPoolConfiguration transportThreadPool;
   private final ThreadPoolConfiguration remoteCommandThreadPool;
   private final ThreadPoolConfiguration totalOrderThreadPool;

   TransportConfiguration(AttributeSet attributes, ThreadPoolConfiguration transportThreadPool,
         ThreadPoolConfiguration remoteCommandThreadPool, ThreadPoolConfiguration totalOrderThreadPool) {
      this.attributes = attributes.checkProtection();
      this.transportThreadPool = transportThreadPool;
      this.remoteCommandThreadPool = remoteCommandThreadPool;
      this.totalOrderThreadPool = totalOrderThreadPool;
   }

   public String clusterName() {
      return attributes.attribute(CLUSTER_NAME).asString();
   }

   public String machineId() {
      return attributes.attribute(MACHINE_ID).asString();
   }

   public String rackId() {
      return attributes.attribute(RACK_ID).asString();
   }

   public String siteId() {
      return attributes.attribute(SITE_ID).asString();
   }

   public long distributedSyncTimeout() {
      return attributes.attribute(DISTRIBUTED_SYNC_TIMEOUT).asLong();
   }

   public Transport transport() {
      return attributes.attribute(TRANSPORT).asObject(Transport.class);
   }

   public String nodeName() {
      return attributes.attribute(NODE_NAME).asString();
   }

   /**
    * @deprecated Since 6.0, strictPeerToPeer is ignored and asymmetric clusters are always allowed.
    */
   @Deprecated
   public boolean strictPeerToPeer() {
      return false;
   }

   public TypedProperties properties() {
      return attributes.attribute(PROPERTIES).asObject(TypedProperties.class);
   }

   public boolean hasTopologyInfo() {
      return siteId() != null || rackId() != null || machineId() != null;
   }

   public ThreadPoolConfiguration transportThreadPool() {
      return transportThreadPool;
   }

   public ThreadPoolConfiguration remoteCommandThreadPool() {
      return remoteCommandThreadPool;
   }

   public ThreadPoolConfiguration totalOrderThreadPool() {
      return totalOrderThreadPool;
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "TransportConfiguration [attributes=" + attributes + ", transportThreadPool=" + transportThreadPool
            + ", remoteCommandThreadPool=" + remoteCommandThreadPool + ", totalOrderThreadPool=" + totalOrderThreadPool
            + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      result = prime * result + ((remoteCommandThreadPool == null) ? 0 : remoteCommandThreadPool.hashCode());
      result = prime * result + ((totalOrderThreadPool == null) ? 0 : totalOrderThreadPool.hashCode());
      result = prime * result + ((transportThreadPool == null) ? 0 : transportThreadPool.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      TransportConfiguration other = (TransportConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      if (remoteCommandThreadPool == null) {
         if (other.remoteCommandThreadPool != null)
            return false;
      } else if (!remoteCommandThreadPool.equals(other.remoteCommandThreadPool))
         return false;
      if (totalOrderThreadPool == null) {
         if (other.totalOrderThreadPool != null)
            return false;
      } else if (!totalOrderThreadPool.equals(other.totalOrderThreadPool))
         return false;
      if (transportThreadPool == null) {
         if (other.transportThreadPool != null)
            return false;
      } else if (!transportThreadPool.equals(other.transportThreadPool))
         return false;
      return true;
   }



}