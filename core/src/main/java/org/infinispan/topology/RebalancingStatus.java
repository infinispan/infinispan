package org.infinispan.topology;

/**
 * RebalancingStatus.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public enum RebalancingStatus {
   SUSPENDED,
   PENDING,
   IN_PROGRESS,
   COMPLETE
}
