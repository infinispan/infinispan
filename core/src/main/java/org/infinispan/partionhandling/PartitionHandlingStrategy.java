package org.infinispan.partionhandling;

/**
 * @author Mircea Markus
 * @since 7.0
 */
public interface PartitionHandlingStrategy {

   /**
    * Implementations might query the PartitionContext in order to determine if this is the primary partition, based on
    * quorum and mark the partition unavailable/readonly.
    */
   void onPartition(PartitionContext pc);
}

