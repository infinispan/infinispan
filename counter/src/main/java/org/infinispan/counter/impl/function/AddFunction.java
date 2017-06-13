package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.logging.Log;
import org.infinispan.util.ByteString;

/**
 * The adding function to update the {@link CounterValue}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class AddFunction<K extends CounterKey> extends BaseFunction<K, CounterValue> {

   public static final AdvancedExternalizer<AddFunction> EXTERNALIZER = new Externalizer();
   private static final Log log = LogFactory.getLog(AddFunction.class, Log.class);
   private final long delta;

   public AddFunction(long delta) {
      this.delta = delta;
   }

   @Override
   void logCounterNotFound(ByteString counterName) {
      log.noSuchCounterAdd(delta, counterName);
   }

   @Override
   CounterValue apply(ReadWriteEntryView<K, CounterValue> entry, ConfigurationMetadata metadata) {
      if (delta == 0) {
         return entry.get();
      }
      if (metadata.get().type() == CounterType.BOUNDED_STRONG) {
         if (this.delta > 0) {
            return addAndCheckUpperBound(entry, metadata);
         } else {
            return addAndCheckLowerBound(entry, metadata);
         }
      } else {
         return addUnbounded(entry, metadata);
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private CounterValue addUnbounded(ReadWriteEntryView<K, CounterValue> entry,
         ConfigurationMetadata configurationMetadata) {
      CounterValue currentValue = entry.get();
      if (noChange(currentValue.getValue())) {
         return currentValue;
      }
      CounterValue newValue;
      try {
         newValue = newCounterValue(Math.addExact(currentValue.getValue(), delta));
      } catch (ArithmeticException e) {
         //overflow!
         newValue = newCounterValue(delta > 0 ? Long.MAX_VALUE : Long.MIN_VALUE);
      }
      entry.set(newValue, configurationMetadata);
      return newValue;
   }

   private CounterValue addAndCheckLowerBound(ReadWriteEntryView<K, CounterValue> entry,
         ConfigurationMetadata metadata) {
      CounterValue currentValue = entry.get();
      if (currentValue.getState() == CounterState.LOWER_BOUND_REACHED) {
         return currentValue;
      }
      CounterValue newValue;
      long lowerBound = metadata.get().lowerBound();
      try {
         long addedValue = Math.addExact(currentValue.getValue(), delta);
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

   private CounterValue addAndCheckUpperBound(ReadWriteEntryView<K, CounterValue> entry,
         ConfigurationMetadata metadata) {
      CounterValue currentValue = entry.get();
      if (currentValue.getState() == CounterState.UPPER_BOUND_REACHED) {
         return currentValue;
      }
      CounterValue newValue;
      long upperBound = metadata.get().upperBound();
      try {
         long addedValue = Math.addExact(currentValue.getValue(), delta);
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

   private boolean noChange(long currentValue) {
      return (currentValue == Long.MAX_VALUE && delta > 0) ||
            (currentValue == Long.MIN_VALUE && delta < 0);
   }

   private static class Externalizer implements AdvancedExternalizer<AddFunction> {

      @Override
      public Set<Class<? extends AddFunction>> getTypeClasses() {
         return Collections.singleton(AddFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ADD_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, AddFunction object) throws IOException {
         output.writeLong(object.delta);
      }

      @Override
      public AddFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new AddFunction(input.readLong());
      }
   }

}
