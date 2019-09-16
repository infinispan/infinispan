package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.functional.EntryView;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;

/**
 * Function that initializes the {@link CounterValue} and {@link ConfigurationMetadata} if they don't exists.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class InitializeCounterFunction<K extends CounterKey> implements
      Function<EntryView.ReadWriteEntryView<K, CounterValue>, CounterValue> {

   public static final AdvancedExternalizer<InitializeCounterFunction> EXTERNALIZER = new Externalizer();
   private final CounterConfiguration counterConfiguration;

   public InitializeCounterFunction(CounterConfiguration counterConfiguration) {
      this.counterConfiguration = counterConfiguration;
   }

   @Override
   public CounterValue apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      Optional<CounterValue> currentValue = entryView.find();
      if (currentValue.isPresent()) {
         return currentValue.get();
      }
      CounterValue newValue = newCounterValue(counterConfiguration);
      entryView.set(newValue, new ConfigurationMetadata(counterConfiguration));
      return newValue;
   }

   @Override
   public String toString() {
      return "InitializeCounterFunction{" +
            "counterConfiguration=" + counterConfiguration +
            '}';
   }

   public static class Externalizer implements AdvancedExternalizer<InitializeCounterFunction> {

      @Override
      public void writeObject(ObjectOutput output, InitializeCounterFunction object) throws IOException {
         CounterConfiguration.EXTERNALIZER.writeObject(output, object.counterConfiguration);
      }

      @Override
      public InitializeCounterFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new InitializeCounterFunction<>(CounterConfiguration.EXTERNALIZER.readObject(input));
      }

      @Override
      public Set<Class<? extends InitializeCounterFunction>> getTypeClasses() {
         return Collections.singleton(InitializeCounterFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.INITIALIZE_FUNCTION;
      }
   }
}
