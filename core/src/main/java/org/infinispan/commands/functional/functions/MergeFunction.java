package org.infinispan.commands.functional.functions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.UserRaisedFunctionalException;

public class MergeFunction<K, V> implements Function<EntryView.ReadWriteEntryView<K, V>, V>, InjectableComponent, Serializable {
   private BiFunction<? super V, ? super V, ? extends V> remappingFunction;
   private V value;
   private Metadata metadata;

   public MergeFunction(V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      this.remappingFunction = remappingFunction;
      this.value = value;
      this.metadata = metadata;
   }

   @Override
   public V apply(EntryView.ReadWriteEntryView<K, V> entry) {
      try {
         V merged = value;
         if (entry.find().isPresent()) {
            merged = remappingFunction.apply(entry.get(), value);
         }
         if (merged == null) {
            entry.set(merged);
         } else {
            entry.set(merged, metadata);
         }
         return merged;
      } catch (Exception ex) {
         throw new UserRaisedFunctionalException(ex);
      }
   }

   @Override
   public void inject(ComponentRegistry registry) {
      registry.wireDependencies(this);
      registry.wireDependencies(remappingFunction);
   }

   public static class Externalizer implements AdvancedExternalizer<MergeFunction> {

      @Override
      public Set<Class<? extends MergeFunction>> getTypeClasses() {
         return Collections.singleton(MergeFunction.class);
      }

      @Override
      public Integer getId() {
         return Ids.MERGE_FUNCTION_MAPPER;
      }

      @Override
      public void writeObject(UserObjectOutput output, MergeFunction object) throws IOException {
         output.writeObject(object.value);
         output.writeObject(object.remappingFunction);
         output.writeObject(object.metadata);
      }

      @Override
      public MergeFunction readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new MergeFunction(input.readObject(),
               (BiFunction) input.readObject(),
               (Metadata) input.readObject());
      }
   }
}
