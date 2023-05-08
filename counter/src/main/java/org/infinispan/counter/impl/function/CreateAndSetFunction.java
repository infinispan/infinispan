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
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;

public class CreateAndSetFunction<K extends CounterKey> extends BaseCreateFunction<K, Object> {

   public static final AdvancedExternalizer<CreateAndSetFunction> EXTERNALIZER = new Externalizer();
   private final long value;

   public CreateAndSetFunction(CounterConfiguration configuration, long value) {
      super(configuration);
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue, CounterConfigurationMetaParam metadata) {
      return FunctionHelper.set(entryView, currentValue, metadata, value);
   }

   private static class Externalizer implements AdvancedExternalizer<CreateAndSetFunction> {
      @Override
      public Set<Class<? extends CreateAndSetFunction>> getTypeClasses() {
         return Collections.singleton(CreateAndSetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CREATE_AND_SET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CreateAndSetFunction object) throws IOException {
         output.writeObject(object.configuration);
         output.writeLong(object.value);
      }

      @Override
      public CreateAndSetFunction<?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CreateAndSetFunction<>((CounterConfiguration) input.readObject(), input.readLong());
      }
   }
}
