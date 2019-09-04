package org.infinispan.counter.impl.entries;

import static org.infinispan.counter.impl.Utils.calculateState;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import net.jcip.annotations.Immutable;

/**
 * Stores the counter's value and {@link CounterState}.
 * <p>
 * If the counter isn't bounded, the state is always {@link CounterState#VALID}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Immutable
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_VALUE)
public class CounterValue {

   //A valid zero value
   private static final CounterValue ZERO = new CounterValue(0, CounterState.VALID);
   private final long value;
   private final CounterState state;

   @ProtoFactory
   CounterValue(long value, CounterState state) {
      this.value = value;
      this.state = state;
   }

   /**
    * Creates a new valid {@link CounterValue} with the value.
    *
    * @param value the counter's value.
    * @return the {@link CounterValue}.
    */
   public static CounterValue newCounterValue(long value) {
      return value == 0 ?
            ZERO :
            new CounterValue(value, CounterState.VALID);
   }

   /**
    * Creates a new {@link CounterValue} with the value and state based on the boundaries.
    *
    * @param value      the counter's value.
    * @param lowerBound the counter's lower bound.
    * @param upperBound the counter's upper bound.
    * @return the {@link CounterValue}.
    */
   public static CounterValue newCounterValue(long value, long lowerBound, long upperBound) {
      return new CounterValue(value, calculateState(value, lowerBound, upperBound));
   }

   /**
    * Creates a new {@link CounterValue} with the value and state.
    *
    * @param value the counter's value.
    * @param state the counter's state.
    * @return the {@link CounterValue}.
    */
   public static CounterValue newCounterValue(long value, CounterState state) {
      return new CounterValue(value, state);
   }

   /**
    * Creates the initial {@link CounterValue} based on {@link CounterConfiguration}.
    *
    * @param configuration the configuration.
    * @return the {@link CounterValue}.
    */
   public static CounterValue newCounterValue(CounterConfiguration configuration) {
      return configuration.type() == CounterType.BOUNDED_STRONG ?
            newCounterValue(configuration.initialValue(), configuration.lowerBound(), configuration.upperBound()) :
            newCounterValue(configuration.initialValue());
   }

   /**
    * Creates the initial {@link CounterValue} based on {@code currentValue} and the {@link CounterConfiguration}.
    *
    * @param currentValue  the current counter's value.
    * @param configuration the configuration.
    * @return the {@link CounterValue}.
    */
   public static CounterValue newCounterValue(long currentValue, CounterConfiguration configuration) {
      return configuration.type() == CounterType.BOUNDED_STRONG ?
             newCounterValue(currentValue, configuration.lowerBound(), configuration.upperBound()) :
             newCounterValue(currentValue);
   }

   /**
    * @return the counter's value.
    */
   @ProtoField(number = 1, defaultValue = "0")
   public long getValue() {
      return value;
   }

   /**
    * @return the counter's state.
    */
   @ProtoField(number = 2)
   public CounterState getState() {
      return state;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      CounterValue that = (CounterValue) o;
      return value == that.value && state == that.state;
   }

   @Override
   public int hashCode() {
      int result = (int) (value ^ (value >>> 32));
      result = 31 * result + state.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "CounterValue{" +
            "value=" + value +
            ", state=" + state +
            '}';
   }
}
