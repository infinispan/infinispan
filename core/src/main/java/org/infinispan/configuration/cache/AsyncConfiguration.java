package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;

/**
 * If configured all communications are asynchronous, in that whenever a thread sends a message sent
 * over the wire, it does not wait for an acknowledgment before returning. Asynchronous configuration is mutually
 * exclusive with synchronous configuration.
 *
 */
public class AsyncConfiguration {
   public static final AttributeDefinition<ReplicationQueue> REPLICATION_QUEUE  = AttributeDefinition.<ReplicationQueue>builder("replicationQueue", null, ReplicationQueue.class).immutable().build();
   public static final AttributeDefinition<Long> REPLICATION_QUEUE_INTERVAL = AttributeDefinition.builder("replicationQueueInterval", 10l).build();
   public static final AttributeDefinition<Integer> REPLICATION_QUEUE_MAX_ELEMENTS  = AttributeDefinition.builder("replicationQueueMaxElements", 1000).build();
   public static final AttributeDefinition<Boolean> USE_REPLICATION_QUEUE = AttributeDefinition.builder("useReplicationQueue", false).immutable().build();

   static final AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AsyncConfiguration.class, REPLICATION_QUEUE, REPLICATION_QUEUE_INTERVAL, REPLICATION_QUEUE_MAX_ELEMENTS, USE_REPLICATION_QUEUE);
   }

   private final Attribute<ReplicationQueue> replicationQueue;
   private final Attribute<Long> replicationQueueInterval;
   private final Attribute<Integer> replicationQueueMaxElements;
   private final Attribute<Boolean> useReplicationQueue;

   private AttributeSet attributes;


   AsyncConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      replicationQueue = attributes.attribute(REPLICATION_QUEUE);
      replicationQueueInterval = attributes.attribute(REPLICATION_QUEUE_INTERVAL);
      replicationQueueMaxElements = attributes.attribute(REPLICATION_QUEUE_MAX_ELEMENTS);
      useReplicationQueue = attributes.attribute(USE_REPLICATION_QUEUE);
   }


   /**
    * Async marshalling has been removed
    */
   @Deprecated
   public boolean asyncMarshalling() {
      return false;
   }

   /**
    * The replication queue in use, by default {@link ReplicationQueueImpl}.
    */
   public ReplicationQueue replQueue() {
      return replicationQueue.get();
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread
    * used to flush the replication queue runs.
    */
   public long replQueueInterval() {
      return replicationQueueInterval.get();
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
    * when it reaches a specific threshold.
    */
   public int replQueueMaxElements() {
      return replicationQueueMaxElements.get();
   }

   /**
    * If true, this forces all async communications to be queued up and sent out periodically as a
    * batch.
    */
   public boolean useReplQueue() {
      return useReplicationQueue.get();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AsyncConfiguration other = (AsyncConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public String toString() {
      return "AsyncConfiguration [attributes=" + attributes + "]";
   }

   public AttributeSet attributes() {
      return attributes;
   }

}
