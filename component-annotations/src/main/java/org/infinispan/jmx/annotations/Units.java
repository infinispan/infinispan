package org.infinispan.jmx.annotations;

public enum Units {

   NONE,

   SECONDS,

   MILLISECONDS,

   MICROSECONDS,

   NANOSECONDS,

   PER_SECOND,

   PERCENTAGE,

   BITS,

   BYTES;

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
