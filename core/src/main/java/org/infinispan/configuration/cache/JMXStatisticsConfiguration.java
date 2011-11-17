package org.infinispan.configuration.cache;

public class JMXStatisticsConfiguration {

   private final boolean enabled;

   JMXStatisticsConfiguration(boolean enabled) {
      this.enabled = enabled;
   }
   
   public boolean isEnabled() {
      return enabled;
   }
   
}
