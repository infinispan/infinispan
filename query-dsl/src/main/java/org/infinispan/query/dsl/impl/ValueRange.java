package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * Represents a range of values starting from value {@code from} and ending at {@code to}, including or excluding
 * the interval ends as indicated by {@code includeLower} and {@code includeUpper} respectively. This is used to
 * represent an interval specified in the 'between' clause of the query DSL.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class ValueRange {

   private static final Log log = Logger.getMessageLogger(Log.class, ValueRange.class.getName());

   private final Object from;

   private final Object to;

   private boolean includeLower = true;

   private boolean includeUpper = true;

   public ValueRange(Object from, Object to) {
      if (!(from instanceof Comparable)) {
         throw log.argumentMustBeComparable("from");
      }
      if (!(to instanceof Comparable)) {
         throw log.argumentMustBeComparable("to");
      }
      this.from = from;
      this.to = to;
   }

   public Object getFrom() {
      return from;
   }

   public Object getTo() {
      return to;
   }

   public boolean isIncludeLower() {
      return includeLower;
   }

   public void setIncludeLower(boolean includeLower) {
      this.includeLower = includeLower;
   }

   public boolean isIncludeUpper() {
      return includeUpper;
   }

   public void setIncludeUpper(boolean includeUpper) {
      this.includeUpper = includeUpper;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(includeLower ? '[' : '(')
            .append(from)
            .append(", ")
            .append(to)
            .append(includeUpper ? ']' : ')');
      return sb.toString();
   }
}
