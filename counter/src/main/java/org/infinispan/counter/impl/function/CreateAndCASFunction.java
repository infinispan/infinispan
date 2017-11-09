package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.functional.EntryView;

/**
 * The compare-and-swap function to update the {@link CounterValue}.
 * <p>
 * It has the same semantic as {@link CompareAndSwapFunction} but it creates the {@link CounterValue} if it doesn't
 * exist.
 *
 * @author Pedro Ruivo
 * @see CompareAndSwapFunction
 * @since 9.2
 */
public class CreateAndCASFunction<K extends CounterKey> extends BaseCreateFunction<K, Object> {

   public static final AdvancedExternalizer<CreateAndCASFunction> EXTERNALIZER = new Externalizer();
   private final long expect;
   private final long value;

   public CreateAndCASFunction(CounterConfiguration configuration, long expect, long value) {
      super(configuration);
      this.expect = expect;
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue,
         ConfigurationMetadata metadata) {
      return FunctionHelper.compareAndSwap(entryView, currentValue, metadata, expect, value);
   }

   private static class Externalizer implements AdvancedExternalizer<CreateAndCASFunction> {

      @Override
      public Set<Class<? extends CreateAndCASFunction>> getTypeClasses() {
         return Collections.singleton(CreateAndCASFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CREATE_CAS_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CreateAndCASFunction object) throws IOException {
         CounterConfiguration.EXTERNALIZER.writeObject(output, object.configuration);
         output.writeLong(object.expect);
         output.writeLong(object.value);
      }

      @Override
      public CreateAndCASFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CreateAndCASFunction(CounterConfiguration.EXTERNALIZER.readObject(input), input.readLong(),
               input.readLong());
      }
   }
}
