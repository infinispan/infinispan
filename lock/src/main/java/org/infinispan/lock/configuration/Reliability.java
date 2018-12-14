package org.infinispan.lock.configuration;

/**
 * Locks are stored in a container that can privilege availability or consistency.
 * Most of the time, locks are both available and consistent.
 * But in some situations, e.g. when the cluster splits, there is a choice between keeping the locks available
 * everywhere (potentially allowing multiple nodes to acquire the same lock) or making it unavailable in the minority
 * partition(s) (potentially requiring administrator intervention to become available again).
 * @see org.infinispan.partitionhandling.PartitionHandling
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
public enum Reliability {
   AVAILABLE,
   CONSISTENT;
}
