package org.infinispan.counter.impl.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.exts.NoStateExternalizer;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.functional.EntryView;

/**
 * It removes the {@link CounterValue} from the cache.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class RemoveFunction<K extends CounterKey> implements
      Function<EntryView.ReadWriteEntryView<K, CounterValue>, Void> {

   public static final AdvancedExternalizer<RemoveFunction> EXTERNALIZER = new Externalizer();

   private static final RemoveFunction INSTANCE = new RemoveFunction();

   private RemoveFunction() {
   }

   public static <K extends CounterKey> RemoveFunction<K> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }


   @Override
   public String toString() {
      return "RemoveFunction{}";
   }

   @Override
   public Void apply(EntryView.ReadWriteEntryView<K, CounterValue> entry) {
      if (entry.find().isPresent()) {
         entry.remove();
      }
      return null;
   }

   private static class Externalizer extends NoStateExternalizer<RemoveFunction> {

      @Override
      public RemoveFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return RemoveFunction.getInstance();
      }

      @Override
      public Set<Class<? extends RemoveFunction>> getTypeClasses() {
         return Collections.singleton(RemoveFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.REMOVE_FUNCTION;
      }
   }
}
