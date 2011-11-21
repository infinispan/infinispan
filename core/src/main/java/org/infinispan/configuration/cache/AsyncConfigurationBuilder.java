package org.infinispan.configuration.cache;

import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;

/**
 * If configured all communications are asynchronous, in that whenever a thread sends a message sent
 * over the wire, it does not wait for an acknowledgment before returning. Asynchronous configuration is mutually
 * exclusive with synchronous configuration.
 *
 */
public class AsyncConfigurationBuilder extends AbstractClusteringConfigurationChildBuilder<AsyncConfiguration> {

   private boolean asyncMarshalling = false;
   private ReplicationQueue replicationQueue = new ReplicationQueueImpl();
   private long replicationQueueInterval = 5000L;
   private int replicationQueueMaxElements = 1000;
   private boolean useReplicationQueue = false;

   protected AsyncConfigurationBuilder(ClusteringConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable asynchronous marshalling. This allows the caller to return even quicker, but it can
    * suffer from reordering of operations. You can find more information at <a
    * href="https://docs.jboss.org/author/display/ISPN/Asynchronous+Options"
    * >https://docs.jboss.org/author/display/ISPN/Asynchronous+Options</a>.
    */
   public AsyncConfigurationBuilder asyncMarshalling() {
      this.asyncMarshalling = true;
      return this;
   }

   /**
    * Enables synchronous marshalling. You can find more information at <a
    * href="https://docs.jboss.org/author/display/ISPN/Asynchronous+Options"
    * >https://docs.jboss.org/author/display/ISPN/Asynchronous+Options</a>.
    */
   public AsyncConfigurationBuilder syncMarshalling() {
      this.asyncMarshalling = false;
      return this;
   }

   /**
    * The replication queue in use, by default {@link ReplicationQueueImpl}.
    */
   public AsyncConfigurationBuilder replQueue(ReplicationQueue replicationQueue) {
      this.replicationQueue = replicationQueue;
      return this;
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread used
    * to flush the replication queue runs.
    */
   public AsyncConfigurationBuilder replQueueInterval(long interval) {
      this.replicationQueueInterval = interval;
      return this;
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
    * when it reaches a specific threshold.
    */
   public AsyncConfigurationBuilder replQueueMaxElements(int elements) {
      this.replicationQueueMaxElements = elements;
      return this;
   }

   /**
    * If true, forces all async communications to be queued up and sent out periodically as a
    * batch.
    */
   public AsyncConfigurationBuilder useReplQueue(boolean use) {
      this.useReplicationQueue = use;
      return this;
   }

   @Override
   void validate() {
      // No-op no validation required

   }

   @Override
   AsyncConfiguration create() {
      return new AsyncConfiguration(asyncMarshalling, replicationQueue, replicationQueueInterval, replicationQueueMaxElements, useReplicationQueue);
   }

}
