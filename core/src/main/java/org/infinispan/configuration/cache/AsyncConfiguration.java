package org.infinispan.configuration.cache;

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

   static final AttributeDefinition<Boolean> MARSHALLING  = AttributeDefinition.builder("asyncMarshalling", false).immutable().build();
   static final AttributeDefinition<ReplicationQueue> REPLICATION_QUEUE  = AttributeDefinition.<ReplicationQueue>builder("replicationQueue", null, ReplicationQueue.class).immutable().build();
   static final AttributeDefinition<Long> REPLICATION_QUEUE_INTERVAL = AttributeDefinition.builder("replicationQueueInterval", 10l).build();
   static final AttributeDefinition<Integer> REPLICATION_QUEUE_MAX_ELEMENTS  = AttributeDefinition.builder("replicationQueueMaxElements", 1000).build();
   static final AttributeDefinition<Boolean> USE_REPLICATION_QUEUE = AttributeDefinition.builder("useReplicationQueue", false).immutable().build();

   static final AttributeSet attributeSet() {
      return new AttributeSet(AsyncConfiguration.class, MARSHALLING, REPLICATION_QUEUE, REPLICATION_QUEUE_INTERVAL, REPLICATION_QUEUE_MAX_ELEMENTS, USE_REPLICATION_QUEUE);
   }
   private AttributeSet attributes;


   AsyncConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * Asynchronous marshalling allows the caller to return even quicker, but it can
    * suffer from reordering of operations. You can find more information at <a
    * href="https://docs.jboss.org/author/display/ISPN/Asynchronous+Options"
    * >https://docs.jboss.org/author/display/ISPN/Asynchronous+Options</a>.
    */
   public boolean asyncMarshalling() {
      return attributes.attribute(MARSHALLING).asBoolean();
   }

   /**
    * The replication queue in use, by default {@link ReplicationQueueImpl}.
    */
   public ReplicationQueue replQueue() {
      return attributes.attribute(REPLICATION_QUEUE).asObject(ReplicationQueue.class);
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread
    * used to flush the replication queue runs.
    */
   public long replQueueInterval() {
      return attributes.attribute(REPLICATION_QUEUE_INTERVAL).asLong();
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
    * when it reaches a specific threshold.
    */
   public int replQueueMaxElements() {
      return attributes.attribute(REPLICATION_QUEUE_MAX_ELEMENTS).asInteger();
   }

   /**
    * If true, this forces all async communications to be queued up and sent out periodically as a
    * batch.
    */
   public boolean useReplQueue() {
      return attributes.attribute(USE_REPLICATION_QUEUE).asBoolean();
   }

   @Override
   public boolean equals(Object o) {
      AsyncConfiguration other = (AsyncConfiguration) o;
      return this.attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   AttributeSet values() {
      return attributes;
   }

}
