package org.infinispan.configuration.cache;

public class LazyDeserializationConfiguration {

   private final boolean enabled;

   LazyDeserializationConfiguration(boolean enabled) {
      this.enabled = enabled;
   }
   
   public boolean isEnabled() {
      return enabled;
   }
   
}
