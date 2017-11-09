package org.infinispan.counter.util;

import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Storage;
import org.infinispan.functional.Param;

/**
 * Utility methods.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class Utils {

   private Utils() {
   }

   /**
    * Checks if the value is inside the boundaries.
    *
    * @param lowerBound the lower bound.
    * @param value      the value to check.
    * @param upperBound the upper bound.
    * @return {@code true} if the value is inside the boundaries, {@code false} otherwise.
    */
   public static boolean isValid(long lowerBound, long value, long upperBound) {
      return lowerBound > value || value > upperBound;
   }

   /**
    * Calculates the {@link CounterState} to use based on the value and the boundaries.
    * <p>
    * If the value is less than the lower bound, {@link CounterState#LOWER_BOUND_REACHED} is returned. On other hand, if
    * the value is higher than the upper bound, {@link CounterState#UPPER_BOUND_REACHED} is returned. Otherwise, {@link
    * CounterState#VALID} is returned.
    *
    * @param value      the value to check.
    * @param lowerBound the lower bound.
    * @param upperBound the upper bound.
    * @return the {@link CounterState}.
    */
   public static CounterState calculateState(long value, long lowerBound, long upperBound) {
      if (value < lowerBound) {
         return CounterState.LOWER_BOUND_REACHED;
      } else if (value > upperBound) {
         return CounterState.UPPER_BOUND_REACHED;
      }
      return CounterState.VALID;
   }

   public static Param.PersistenceMode getPersistenceMode(Storage storage) {
      switch (storage) {
         case PERSISTENT:
            return Param.PersistenceMode.PERSIST;
         case VOLATILE:
            return Param.PersistenceMode.SKIP;
         default:
            throw new IllegalStateException("[should never happen] unknown storage " + storage);
      }
   }
}
