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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JMXStatisticsConfiguration that = (JMXStatisticsConfiguration) o;

      if (enabled != that.enabled) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return (enabled ? 1 : 0);
   }

}
