package org.infinispan.configuration.cache;

import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;

/**
 * If configured all communications are asynchronous, in that whenever a thread sends a message sent
 * over the wire, it does not wait for an acknowledgment before returning. Asynchronous configuration is mutually
 * exclusive with synchronous configuration.
 *
 */
public class AsyncConfiguration {

   private final boolean asyncMarshalling;
   private final ReplicationQueue replicationQueue;
   private final long replicationQueueInterval;
   private final int replicationQueueMaxElements;
   private final boolean useReplicationQueue;

   AsyncConfiguration(boolean asyncMarshalling, ReplicationQueue replicationQueue, long replicationQueueInterval,
         int replicationQueueMaxElements, boolean useReplicationQueue) {
      this.asyncMarshalling = asyncMarshalling;
      this.replicationQueue = replicationQueue;
      this.replicationQueueInterval = replicationQueueInterval;
      this.replicationQueueMaxElements = replicationQueueMaxElements;
      this.useReplicationQueue = useReplicationQueue;
   }

   /**
    * Asynchronous marshalling allows the caller to return even quicker, but it can
    * suffer from reordering of operations. You can find more information at <a
    * href="https://docs.jboss.org/author/display/ISPN/Asynchronous+Options"
    * >https://docs.jboss.org/author/display/ISPN/Asynchronous+Options</a>.
    */
   public boolean isAsyncMarshalling() {
      return asyncMarshalling;
   }

   /**
    * The replication queue in use, by default {@link ReplicationQueueImpl}.
    */
   public ReplicationQueue getReplQueue() {
      return replicationQueue;
   }

   /**
    * If useReplQueue is set to true, this attribute controls how often the asynchronous thread
    * used to flush the replication queue runs.
    */
   public long getReplQueueInterval() {
      return replicationQueueInterval;
   }

   /**
    * If useReplQueue is set to true, this attribute can be used to trigger flushing of the queue
    * when it reaches a specific threshold.
    */
   public int getReplQueueMaxElements() {
      return replicationQueueMaxElements;
   }

   /**
    * If true, this forces all async communications to be queued up and sent out periodically as a
    * batch.
    */
   public boolean isUseReplQueue() {
      return useReplicationQueue;
   }

}
