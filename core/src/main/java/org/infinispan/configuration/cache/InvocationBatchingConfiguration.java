package org.infinispan.configuration.cache;

public class InvocationBatchingConfiguration {

   private final boolean enabled;

   InvocationBatchingConfiguration(boolean enabled) {
      this.enabled = enabled;
   }
   
   public boolean isEnabled() {
      return enabled;
   }
   
}
