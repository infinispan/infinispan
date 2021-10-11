package org.infinispan.cli.completers;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
public class AvailabilityCompleter extends EnumCompleter<AvailabilityCompleter.Availability> {
   public enum Availability {
      AVAILABLE,
      DEGRADED_MODE,
   }

   public AvailabilityCompleter() {
      super(AvailabilityCompleter.Availability.class);
   }
}
