package org.infinispan.jmx.annotations;

/**
 * The type of data that is to be collected. This type also effects the way the data is exported for management and for
 * metrics.
 */
public enum DataType {

   /**
    * Numeric data that normally changes over time. If this is applied to a non-numeric property it will not be exported
    * as a metric but will still be exported as a manageable attribute.
    */
   MEASUREMENT,

   /**
    * A value of the running system that changes rarely or never. This is exposed as a manageable attribute, but never
    * as a metric.
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
