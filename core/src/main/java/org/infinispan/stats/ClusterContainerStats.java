package org.infinispan.stats;

/**
 * Cluster wide container statistics
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public interface ClusterContainerStats extends ClusterStats {
   /**
    * @return the maximum amount of free memory in bytes across the cluster JVMs.
    */
   long getMemoryAvailable();

   /**
    * @return the maximum amount of memory that JVMs across the cluster will attempt to utilise in bytes.
    */
   long getMemoryMax();

   /**
    * @return the total amount of memory in the JVMs across the cluster in bytes.
    */
   long getMemoryTotal();

   /**
    * @return the amount of memory used by JVMs across the cluster in bytes.
    */
   long getMemoryUsed();
}
