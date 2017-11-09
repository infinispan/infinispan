package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.CounterState;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView;

/**
 * The compare-and-swap function to update the {@link CounterValue}.
 * <p>
 * It returns the previous value and it is considered successful when the return value is the {@code expect}ed.
 * <p>
 * For a bounded counter (if the current value is equal to the {@code expect}ed), if the {@code value} is outside the
 * bounds, it returns {@link CounterState#LOWER_BOUND_REACHED} or {@link CounterState#UPPER_BOUND_REACHED} if the lower
 * bound or upper bound is violated.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CompareAndSwapFunction<K extends CounterKey> extends BaseFunction<K, Object> {

   public static final AdvancedExternalizer<CompareAndSwapFunction> EXTERNALIZER = new Externalizer();
   private static final Log log = LogFactory.getLog(CompareAndSwapFunction.class, Log.class);
   private final long expect;
   private final long value;

   public CompareAndSwapFunction(long expect, long value) {
      this.expect = expect;
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, ConfigurationMetadata metadata) {
      return FunctionHelper.compareAndSwap(entryView, entryView.get(), metadata, expect, value);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private static class Externalizer implements AdvancedExternalizer<CompareAndSwapFunction> {

      @Override
      public Set<Class<? extends CompareAndSwapFunction>> getTypeClasses() {
         return Collections.singleton(CompareAndSwapFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CAS_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CompareAndSwapFunction object) throws IOException {
         output.writeLong(object.expect);
         output.writeLong(object.value);
      }

      @Override
      public CompareAndSwapFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CompareAndSwapFunction(input.readLong(), input.readLong());
      }
   }
}
