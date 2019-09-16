package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.functional.EntryView;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.exts.NoStateExternalizer;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;

/**
 * Read function that returns the current counter's delta.
 * <p>
 * Singleton class. Use {@link ReadFunction#getInstance()} to retrieve it.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class ReadFunction<K> implements Function<EntryView.ReadEntryView<K, CounterValue>, Long> {

   public static final AdvancedExternalizer<ReadFunction> EXTERNALIZER = new Externalizer();
   private static final ReadFunction INSTANCE = new ReadFunction();

   private ReadFunction() {
   }

   public static <K> ReadFunction<K> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   @Override
   public String toString() {
      return "ReadFunction{}";
   }

   @Override
   public Long apply(EntryView.ReadEntryView<K, CounterValue> view) {
      return view.find().map(CounterValue::getValue).orElse(null);
   }

   public static class Externalizer extends NoStateExternalizer<ReadFunction> {

      @Override
      public Set<Class<? extends ReadFunction>> getTypeClasses() {
         return Collections.singleton(ReadFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.READ_FUNCTION;
      }

      @Override
      public ReadFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return INSTANCE;
      }
   }
}
