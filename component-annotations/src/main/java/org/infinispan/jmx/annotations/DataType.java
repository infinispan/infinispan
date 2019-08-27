package org.infinispan.jmx.annotations;

/**
 * The type of data that is to be collected.
 */
public enum DataType {

   /**
    * Numeric data that normally changes rapidly over time.
    */
   MEASUREMENT,

   /**
    * A value of the running system that changes rarely or never.
    */
   TRAIT,

   /**
    * Same as {@link #MEASUREMENT}, but instead of instantaneous values it provides a histogram of past values.
    */
   HISTOGRAM;

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
