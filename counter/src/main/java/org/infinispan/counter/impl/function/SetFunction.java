package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;

/**
 * The function to set the {@link CounterValue}.
 *
 * @author Dipanshu Gupta
 * @since 15.0
 */
public final class SetFunction<K extends CounterKey> extends BaseFunction<K, Object> implements Serializable {
   public static final AdvancedExternalizer<SetFunction> EXTERNALIZER = new Externalizer();
   private static final Log log = LogFactory.getLog(SetFunction.class, Log.class);

   private final long value;

   public SetFunction(long value) {
      this.value = value;
   }

   @Override
   Object apply(EntryView.ReadWriteEntryView<K, CounterValue> entry, CounterConfigurationMetaParam metadata) {
      return FunctionHelper.set(entry, entry.get(), metadata, value);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private static class Externalizer implements AdvancedExternalizer<SetFunction> {

      @Override
      public Set<Class<? extends SetFunction>> getTypeClasses() {
         return Collections.singleton(SetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.SET_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SetFunction object) throws IOException {
         output.writeLong(object.value);
      }

      @Override
      public SetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SetFunction(input.readLong());
      }
   }
}
