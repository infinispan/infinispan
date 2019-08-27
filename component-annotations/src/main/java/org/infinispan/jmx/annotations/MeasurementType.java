package org.infinispan.jmx.annotations;

/**
 * Indicates whether the measured values consistently increase or decrease over time or are dynamic and can "randomly"
 * be higher or lower than previous values.
 */
public enum MeasurementType {

   DYNAMIC, TRENDSUP, TRENDSDOWN;

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
