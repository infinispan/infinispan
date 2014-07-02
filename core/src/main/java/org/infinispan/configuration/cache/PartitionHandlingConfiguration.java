package org.infinispan.configuration.cache;

public class PartitionHandlingConfiguration {

   private final boolean enabled;

   public PartitionHandlingConfiguration(boolean enabled) {
      this.enabled = enabled;
   }

   public boolean enabled() {return enabled;}

}
