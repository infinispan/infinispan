package org.infinispan.jmx.annotations;

public enum DataType {
   MEASUREMENT, TRAIT, CALLTIME;

   @Override
   public String toString() {
      return this.name().toLowerCase();
   }

}
