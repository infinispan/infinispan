package org.infinispan.stats;

/**
 * Cluster wide container statistics.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public interface ClusterContainerStats extends ContainerStats, ClusterStats {

   String OBJECT_NAME = "ClusterContainerStats";
}
