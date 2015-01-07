package org.infinispan.partitionhandling;

/**
* @author Mircea Markus
* @author Dan Berindei
* @since 7.0
*/
public enum AvailabilityMode {
   /** Regular operation mode */
   AVAILABLE,
   /** Data can not be safely accessed because of a network split or successive nodes leaving. */
   DEGRADED_MODE,
   /** @deprecated Use {@link #DEGRADED_MODE} instead. */
   UNAVAILABLE,
   ;

   public AvailabilityMode min(AvailabilityMode other) {
      if (this == UNAVAILABLE || other == UNAVAILABLE)
         return UNAVAILABLE;
      if (this == DEGRADED_MODE || other == DEGRADED_MODE)
         return DEGRADED_MODE;
      return AVAILABLE;
   }
}
