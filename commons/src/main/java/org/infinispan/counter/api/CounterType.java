package org.infinispan.counter.api;

import org.infinispan.counter.exception.CounterOutOfBoundsException;

/**
 * The counter types.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public enum CounterType {
   /**
    * A strong consistent and unbounded counter. The counter will never throw a {@link
    * CounterOutOfBoundsException}.
    */
   UNBOUNDED_STRONG,
   /**
    * A strong consistent and bounded counter. The counter will throw {@link CounterOutOfBoundsException}
    * if the boundaries are reached. The upper and lower bound are inclusive.
    */
   BOUNDED_STRONG,
   /**
    * A weak consistent counter. It focus on write performance and its counter value is only calculated at read time.
    */
   WEAK;

   private static final CounterType[] CACHED_VALUES = values();

   public static CounterType valueOf(int index) {
      return CACHED_VALUES[index];
   }
}
