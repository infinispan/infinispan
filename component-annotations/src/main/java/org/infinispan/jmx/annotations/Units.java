package org.infinispan.jmx.annotations;

import java.util.EnumSet;

public enum Units {

   NONE,

   SECONDS,

   MILLISECONDS,

   MICROSECONDS,

   NANOSECONDS,

   PER_SECOND,

   PERCENTAGE,

   BITS,

   BYTES,

   KILO_BYTES,

   MEGA_BYTES;

   public static final EnumSet<Units> TIME_UNITS = EnumSet.of(SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS);

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
