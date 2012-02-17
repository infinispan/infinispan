package org.infinispan.configuration.cache;

public class InvocationBatchingConfiguration {

   private final boolean enabled;

   InvocationBatchingConfiguration(boolean enabled) {
      this.enabled = enabled;
   }
   
   public boolean enabled() {
      return enabled;
   }

   @Override
   public String toString() {
      return "InvocationBatchingConfiguration{" +
            "enabled=" + enabled +
            '}';
   }

}
