package org.infinispan.counter.impl.listener;

import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.impl.entries.CounterValue;

import static java.util.Objects.requireNonNull;

/**
 * The {@link CounterEvent} implementation.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterEventImpl implements CounterEvent {

   private final long oldValue;
   private final CounterState oldState;
   private final long newValue;
   private final CounterState newState;

   private CounterEventImpl(long oldValue, CounterState oldState, long newValue, CounterState newState) {
      this.oldValue = oldValue;
      this.oldState = requireNonNull(oldState);
      this.newValue = newValue;
      this.newState = requireNonNull(newState);
   }

   public static CounterEvent create(long oldValue, long newValue) {
      return new CounterEventImpl(oldValue, CounterState.VALID, newValue, CounterState.VALID);
   }

   public static CounterEvent create(CounterValue oldValue, CounterValue newValue) {
      if (oldValue == null) {
         return new CounterEventImpl(newValue.getValue(), newValue.getState(), newValue.getValue(), newValue.getState());
      }
      return new CounterEventImpl(oldValue.getValue(), oldValue.getState(), newValue.getValue(), newValue.getState());
   }

   @Override
   public long getOldValue() {
      return oldValue;
   }

   @Override
   public CounterState getOldState() {
      return oldState;
   }

   @Override
   public long getNewValue() {
      return newValue;
   }

   @Override
   public CounterState getNewState() {
      return newState;
   }

   @Override
   public String toString() {
      return "CounterEventImpl{" +
            "oldValue=" + oldValue +
            ", oldState=" + oldState +
            ", newValue=" + newValue +
            ", newState=" + newState +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CounterEventImpl that = (CounterEventImpl) o;

      return oldValue == that.oldValue &&
            newValue == that.newValue &&
            oldState == that.oldState &&
            newState == that.newState;

   }

   @Override
   public int hashCode() {
      int result = (int) (oldValue ^ (oldValue >>> 32));
      result = 31 * result + oldState.hashCode();
      result = 31 * result + (int) (newValue ^ (newValue >>> 32));
      result = 31 * result + newState.hashCode();
      return result;
   }
}
