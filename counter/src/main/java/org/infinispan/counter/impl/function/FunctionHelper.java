package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.functional.EntryView;

/**
 * A helper with the function logic.
 * <p>
 * It avoids duplicate code between the functions and the create-functions.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
final class FunctionHelper {

   private FunctionHelper() {
   }

   /**
    * Performs a compare-and-swap and return the previous value.
    * <p>
    * The compare-and-swap is successful if the return value is equals to the {@code expected}.
    *
    * @return the previous value or {@link CounterState#UPPER_BOUND_REACHED}/{@link CounterState#LOWER_BOUND_REACHED} if
    * it reaches the boundaries of a bounded counter.
    */
   static Object compareAndSwap(EntryView.ReadWriteEntryView<?, CounterValue> entry,
         CounterValue value, CounterConfigurationMetaParam metadata, long expected, long update) {
      long retVal = value.getValue();
      if (expected == retVal) {
         if (metadata.get().type() == CounterType.BOUNDED_STRONG) {
            if (update < metadata.get().lowerBound()) {
               return CounterState.LOWER_BOUND_REACHED;
            } else if (update > metadata.get().upperBound()) {
               return CounterState.UPPER_BOUND_REACHED;
            }
         }
         entry.set(CounterValue.newCounterValue(update, CounterState.VALID), metadata);
         return retVal;

      } else {
         return retVal;
      }
   }

   static CounterValue add(EntryView.ReadWriteEntryView<?, CounterValue> entry,
         CounterValue value, CounterConfigurationMetaParam metadata, long delta) {
      if (delta == 0) {
         return value;
      }
      if (metadata.get().type() == CounterType.BOUNDED_STRONG) {
         if (delta > 0) {
            return addAndCheckUpperBound(entry, value, metadata, delta);
         } else {
            return addAndCheckLowerBound(entry, value, metadata, delta);
         }
      } else {
         return addUnbounded(entry, value, metadata, delta);
      }
   }

   private static CounterValue addAndCheckUpperBound(EntryView.ReadWriteEntryView<?, CounterValue> entry,
         CounterValue value, CounterConfigurationMetaParam metadata, long delta) {
      if (value.getState() == CounterState.UPPER_BOUND_REACHED) {
         return value;
      }
      CounterValue newValue;
      long upperBound = metadata.get().upperBound();
      try {
         long addedValue = Math.addExact(value.getValue(), delta);
         if (addedValue > upperBound) {
            newValue = newCounterValue(upperBound, CounterState.UPPER_BOUND_REACHED);
         } else {
            newValue = newCounterValue(addedValue, CounterState.VALID);
         }
      } catch (ArithmeticException e) {
         //overflow!
         newValue = newCounterValue(Long.MAX_VALUE, CounterState.UPPER_BOUND_REACHED);
      }
      entry.set(newValue, metadata);
      return newValue;
   }

   private static CounterValue addAndCheckLowerBound(EntryView.ReadWriteEntryView<?, CounterValue> entry,
         CounterValue value, CounterConfigurationMetaParam metadata, long delta) {
      if (value.getState() == CounterState.LOWER_BOUND_REACHED) {
         return value;
      }
      CounterValue newValue;
      long lowerBound = metadata.get().lowerBound();
      try {
         long addedValue = Math.addExact(value.getValue(), delta);
         if (addedValue < lowerBound) {
            newValue = newCounterValue(lowerBound, CounterState.LOWER_BOUND_REACHED);
         } else {
            newValue = newCounterValue(addedValue, CounterState.VALID);
         }
      } catch (ArithmeticException e) {
         //overflow!
         newValue = newCounterValue(Long.MIN_VALUE, CounterState.LOWER_BOUND_REACHED);
      }
      entry.set(newValue, metadata);
      return newValue;
   }

   private static CounterValue addUnbounded(EntryView.ReadWriteEntryView<?, CounterValue> entry, CounterValue value,
         CounterConfigurationMetaParam metadata, long delta) {
      if (noChange(value.getValue(), delta)) {
         return value;
      }
      CounterValue newValue;
      try {
         newValue = newCounterValue(Math.addExact(value.getValue(), delta));
      } catch (ArithmeticException e) {
         //overflow!
         newValue = newCounterValue(delta > 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
      }
      entry.set(newValue, metadata);
      return newValue;
   }

   private static boolean noChange(long currentValue, long delta) {
      return (currentValue == Long.MAX_VALUE && delta > 0) ||
            (currentValue == Long.MIN_VALUE && delta < 0);
   }
}
