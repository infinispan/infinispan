package org.infinispan.jmx.annotations;

public enum Units {
   NONE, MILLISECONDS, SECONDS, PERCENTAGE, NANOSECONDS;

   @Override
   public String toString() {
      return this.name().toLowerCase();
   }

}
