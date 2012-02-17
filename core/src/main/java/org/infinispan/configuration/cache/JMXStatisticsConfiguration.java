package org.infinispan.configuration.cache;

/**
 * Determines whether statistics are gather and reported.
 * 
 * @author pmuir
 *
 */
public class JMXStatisticsConfiguration {

   private final boolean enabled;

   /**
    * Enable or disable statistics gathering and reporting
    * 
    * @param enabled
    */
   JMXStatisticsConfiguration(boolean enabled) {
      this.enabled = enabled;
   }
   
   public boolean enabled() {
      return enabled;
   }

   @Override
   public String toString() {
      return "JMXStatisticsConfiguration{" +
            "enabled=" + enabled +
            '}';
   }

}
