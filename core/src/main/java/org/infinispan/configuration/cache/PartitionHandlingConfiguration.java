package org.infinispan.configuration.cache;

/**
 * Controls how the cache handles partitioning and/or multiple node failures.
 *
 * @author Mircea Markus
 * @since 7.0
 */
public class PartitionHandlingConfiguration {

   private final boolean enabled;

   public PartitionHandlingConfiguration(boolean enabled) {
      this.enabled = enabled;
   }

   public boolean enabled() {return enabled;}

}
