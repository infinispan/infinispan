package org.infinispan.jmx.annotations;

public enum Units {
   NONE, NANOSECONDS, MILLISECONDS, SECONDS, PERCENTAGE;

   @Override
   public String toString() {
      return this.name().toLowerCase();
   }

}
