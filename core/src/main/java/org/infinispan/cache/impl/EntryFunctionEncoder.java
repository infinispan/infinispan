package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public record EntryFunctionEncoder<K, V, O>(Function<Publisher<CacheEntry<K, V>>, O> innerFunction,
                                            EncoderEntryMapper<K, V, CacheEntry<K, V>> mapper) implements Function<Publisher<CacheEntry<K, V>>, O>, InjectableComponent {

   @Override
   public O apply(Publisher<CacheEntry<K, V>> iPublisher) {
      return innerFunction.apply(
            Flowable.fromPublisher(iPublisher)
                  .map(mapper::apply));
   }

   @Override
   public void inject(ComponentRegistry registry) {
      mapper.inject(registry);
   }

   public static class EntryFunctionExternalizer implements AdvancedExternalizer<EntryFunctionEncoder> {

      @Override
      public Set<Class<? extends EntryFunctionEncoder>> getTypeClasses() {
         return Collections.singleton(EntryFunctionEncoder.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODER_ENTRY_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, EntryFunctionEncoder object) throws IOException {
         output.writeObject(object.innerFunction);
         output.writeObject(object.mapper);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EntryFunctionEncoder readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EntryFunctionEncoder((Function) input.readObject(), (EncoderEntryMapper) input.readObject());
      }
   }
}
