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
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public record KeyFunctionEncoder<I, O>(Function<Publisher<I>, O> innerFunction,
                                DataConversion keyDataConversion) implements Function<Publisher<I>, O>, InjectableComponent {

   @Override
   public O apply(Publisher<I> iPublisher) {
      return innerFunction.apply(
            Flowable.fromPublisher(iPublisher)
                  .map(k -> (I) keyDataConversion.fromStorage(k)));
   }

   @Override
   public void inject(ComponentRegistry registry) {
      registry.wireDependencies(keyDataConversion);
   }

   public static class KeyFunctionExternalizer implements AdvancedExternalizer<KeyFunctionEncoder> {

      @Override
      public Set<Class<? extends KeyFunctionEncoder>> getTypeClasses() {
         return Collections.singleton(KeyFunctionEncoder.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODER_KEY_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, KeyFunctionEncoder object) throws IOException {
         output.writeObject(object.innerFunction);
         DataConversion.writeTo(output, object.keyDataConversion);
      }

      @Override
      @SuppressWarnings("unchecked")
      public KeyFunctionEncoder readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyFunctionEncoder((Function) input.readObject(), DataConversion.readFrom(input));
      }
   }
}
