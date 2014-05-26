package org.infinispan.configuration.cache;

import org.infinispan.partionhandling.PartitionHandlingStrategy;

public class PartitionHandlingConfiguration {

   private final PartitionHandlingStrategy partitionHandlingStrategy;
   private final boolean enabled;

   public PartitionHandlingConfiguration(PartitionHandlingStrategy partitionHandlingStrategy, boolean enabled) {
      this.partitionHandlingStrategy = partitionHandlingStrategy;
      this.enabled = enabled;
   }

   public PartitionHandlingStrategy strategy() {
      return partitionHandlingStrategy;
   }

   public boolean enabled() {return enabled;}

}
