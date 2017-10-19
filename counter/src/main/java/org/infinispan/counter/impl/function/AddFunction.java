package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView.ReadWriteEntryView;

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
   CounterValue apply(ReadWriteEntryView<K, CounterValue> entry, ConfigurationMetadata metadata) {
      return FunctionHelper.add(entry, entry.get(), metadata, delta);
   }

   @Override
   protected Log getLog() {
      return log;
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
