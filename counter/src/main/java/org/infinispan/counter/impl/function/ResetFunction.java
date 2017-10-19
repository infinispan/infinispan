package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.functional.EntryView;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.exts.NoStateExternalizer;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.logging.Log;

/**
 * Reset function that sets the counter's delta to it's initial delta.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ResetFunction<K extends CounterKey> extends BaseFunction<K, Void> {

   public static final AdvancedExternalizer<ResetFunction> EXTERNALIZER = new Externalizer();
   private static final Log log = LogFactory.getLog(ResetFunction.class, Log.class);

   private static final ResetFunction INSTANCE = new ResetFunction();

   private ResetFunction() {
   }

   public static <K extends CounterKey> ResetFunction<K> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   @Override
   Void apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, ConfigurationMetadata metadata) {
      entryView.set(newCounterValue(metadata.get()), metadata);
      return null;
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public String toString() {
      return "ResetFunction{}";
   }

   private static class Externalizer extends NoStateExternalizer<ResetFunction> {

      @Override
      public ResetFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return ResetFunction.getInstance();
      }

      @Override
      public Set<Class<? extends ResetFunction>> getTypeClasses() {
         return Collections.singleton(ResetFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.RESET_FUNCTION;
      }
   }
}
