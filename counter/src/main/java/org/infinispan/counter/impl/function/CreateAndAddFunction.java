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
 * The adding function to update the {@link CounterValue}.
 * <p>
 * If the {@link CounterValue} doesn't exist, it is created. This is a difference between {@link AddFunction} and this
 * class.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CreateAndAddFunction<K extends CounterKey> extends BaseCreateFunction<K, CounterValue> {

   public static final AdvancedExternalizer<CreateAndAddFunction> EXTERNALIZER = new Externalizer();
   private final long delta;

   public CreateAndAddFunction(CounterConfiguration configuration, long delta) {
      super(configuration);
      this.delta = delta;
   }

   @Override
   CounterValue apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue,
         ConfigurationMetadata metadata) {
      return FunctionHelper.add(entryView, currentValue, metadata, delta);
   }

   private static class Externalizer implements AdvancedExternalizer<CreateAndAddFunction> {

      @Override
      public Set<Class<? extends CreateAndAddFunction>> getTypeClasses() {
         return Collections.singleton(CreateAndAddFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CREATE_ADD_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CreateAndAddFunction object) throws IOException {
         CounterConfiguration.EXTERNALIZER.writeObject(output, object.configuration);
         output.writeLong(object.delta);
      }

      @Override
      public CreateAndAddFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CreateAndAddFunction(CounterConfiguration.EXTERNALIZER.readObject(input), input.readLong());
      }
   }
}
