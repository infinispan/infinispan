package org.infinispan.counter.util;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.Param;

/**
 * Utility methods.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class Utils {

   //use utils log?
   private static final Log log = LogFactory.getLog(Utils.class, Log.class);

   private Utils() {
   }

   /**
    * Validates the lower and upper bound for a strong counter.
    * <p>
    * It throws a {@link CounterConfigurationException} is not valid.
    *
    * @param lowerBound   The counter's lower bound value.
    * @param initialValue The counter's initial value.
    * @param upperBound   The counter's upper bound value.
    * @throws CounterConfigurationException if the upper or lower bound aren't valid.
    */
   public static void validateStrongCounterBounds(long lowerBound, long initialValue, long upperBound) {
      if (lowerBound > initialValue || initialValue > upperBound) {
         throw log.invalidInitialValueForBoundedCounter(lowerBound, upperBound, initialValue);
      } else if (lowerBound == upperBound) {
         throw log.invalidSameLowerAndUpperBound(lowerBound, upperBound);
      }
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
            return Param.PersistenceMode.LOAD_PERSIST;
         case VOLATILE:
            return Param.PersistenceMode.SKIP;
         default:
            throw new IllegalStateException("[should never happen] unknown storage " + storage);
      }
   }
}
