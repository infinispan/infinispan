package org.infinispan.objectfilter.impl.aggregation;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
public final class Counter {

   private long counter;

   public Counter() {
   }

   public void inc() {
      counter++;
   }

   public void add(long val) {
      counter += val;
   }

   public long getValue() {
      return counter;
   }

   @Override
   public String toString() {
      return "Counter(" + counter + ')';
   }
}
