package org.infinispan.jmx.annotations;

/**
 * The type of data that is to be collected. This type also effects the way the data is exported for management and for
 * metrics.
 */
public enum DataType {

   /**
    * A value of the running system that changes rarely or never. This is exposed as a JMX manageable attribute, but
    * never as a metric.
    */
   TRAIT,

   /**
    * Numeric data that normally changes over time and it interesting to be exposed as a metric. If this is applied to a
    * non-numeric property it will not be exported as a metric but will still be exported as a JMX manageable
    * attribute.
    */
   MEASUREMENT,

   /**
    * Similar to {@link #MEASUREMENT}, but instead of instantaneous values this provides a histogram of past values.
    */
   HISTOGRAM,

   /**
    * Similar to {@link #HISTOGRAM} but the measured values are time durations.
    */
   TIMER;

   @Override
   public String toString() {
      return name().toLowerCase();
   }
}
