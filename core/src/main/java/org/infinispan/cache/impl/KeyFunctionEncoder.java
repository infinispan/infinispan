package org.infinispan.cache.impl;

import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

@ProtoTypeId(ProtoStreamTypeIds.KEY_FUNCTION_ENCODER)
public class KeyFunctionEncoder<I, O> implements Function<Publisher<I>, O>, InjectableComponent {

   final Function<Publisher<I>, O> innerFunction;

   @ProtoField(1)
   final DataConversion keyDataConversion;

   KeyFunctionEncoder(Function<Publisher<I>, O> innerFunction, DataConversion keyDataConversion) {
      this.innerFunction = innerFunction;
      this.keyDataConversion = keyDataConversion;
   }

   @ProtoFactory
   KeyFunctionEncoder(MarshallableObject<Function<Publisher<I>, O>> innerFunction, DataConversion keyDataConversion) {
      this(MarshallableObject.unwrap(innerFunction), keyDataConversion);
   }

   @ProtoField(2)
   MarshallableObject<Function<Publisher<I>, O>> getInnerFunction() {
      return MarshallableObject.create(innerFunction);
   }

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
}
