package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.functional.EntryView;
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
 * The compare-and-set function to update the {@link CounterValue}.
 * <p>
 * If the value is different from {@code expect}, it returns {@code null}.
 * <p>
 * For a bounded counter, if the {@code value} is outside the bounds, it returns {@link
 * CounterState#LOWER_BOUND_REACHED} or {@link CounterState#UPPER_BOUND_REACHED} if the lower bound or upper bound is
 * violated.
 * <p>
 * If the compare-and-set is successful, it returns  {@link CounterState#VALID}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CompareAndSetFunction<K extends CounterKey> extends BaseFunction<K, CounterState> {

   public static final AdvancedExternalizer<CompareAndSetFunction> EXTERNALIZER = new Externalizer();
   private static final Log log = LogFactory.getLog(CompareAndSetFunction.class, Log.class);
   private final long expect;
   private final long value;

   public CompareAndSetFunction(long expect, long value) {
      this.expect = expect;
      this.value = value;
   }

   @Override
   void logCounterNotFound(ByteString counterName) {
      log.noSuchCounterCAS(expect, value, counterName);
   }

   @Override
   CounterState apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, ConfigurationMetadata metadata) {
      CounterValue existing = entryView.get();
      if (expect == existing.getValue()) {
         if (metadata.get().type() == CounterType.BOUNDED_STRONG) {
            if (value < metadata.get().lowerBound()) {
               return CounterState.LOWER_BOUND_REACHED;
            } else if (value > metadata.get().upperBound()) {
               return CounterState.UPPER_BOUND_REACHED;
            }
         }
         entryView.set(CounterValue.newCounterValue(value, CounterState.VALID), metadata);
         return CounterState.VALID;

      } else {
         return null;
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private static class Externalizer implements AdvancedExternalizer<CompareAndSetFunction> {

      @Override
      public Set<Class<? extends CompareAndSetFunction>> getTypeClasses() {
         return Collections.singleton(CompareAndSetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CAS_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CompareAndSetFunction object) throws IOException {
         output.writeLong(object.expect);
         output.writeLong(object.value);
      }

      @Override
      public CompareAndSetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CompareAndSetFunction(input.readLong(), input.readLong());
      }
   }
}
