package org.infinispan.query.dsl.impl;

/**
 * Represents a range of values starting from value {@code from} and ending at {@code to}, including or excluding
 * the interval ends as indicated by {@code includeLower} and {@code includeUpper} respectively. This is used to
 * represent an interval specified in the 'between' clause of the query DSL.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
class ValueRange {

   private Object from;

   private Object to;

   private boolean includeLower = true;

   private boolean includeUpper = true;

   public ValueRange(Object from, Object to) {
      this.from = from;
      this.to = to;
   }

   public Object getFrom() {
      return from;
   }

   public void setFrom(Object from) {
      this.from = from;
   }

   public Object getTo() {
      return to;
   }

   public void setTo(Object to) {
      this.to = to;
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
      sb.append(includeLower ? '[' : '(');
      sb.append(from);
      sb.append(", ");
      sb.append(to);
      sb.append(includeUpper ? ']' : ')');
      return sb.toString();
   }
}
