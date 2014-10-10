package org.infinispan.partionhandling;

/**
* @author Mircea Markus
* @author Dan Berindei
* @since 7.0
*/
public enum AvailabilityMode {
   /** Regular operation mode */
   AVAILABLE,
   /** Data can not be safely accessed because of a network split */
   DEGRADED_MODE,
   /** Data has been lost permanently because of successive nodes leaving */
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
