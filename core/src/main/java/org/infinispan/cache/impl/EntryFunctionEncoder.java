package org.infinispan.cache.impl;

import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

@ProtoTypeId(ProtoStreamTypeIds.ENTRY_FUNCTION_ENCODER)
public class EntryFunctionEncoder<K, V, O> implements Function<Publisher<CacheEntry<K, V>>, O>, InjectableComponent {

   final Function<Publisher<CacheEntry<K, V>>, O> innerFunction;

   @ProtoField(1)
   final EncoderEntryMapper<K, V, CacheEntry<K, V>> mapper;

   EntryFunctionEncoder(Function<Publisher<CacheEntry<K, V>>, O> innerFunction, EncoderEntryMapper<K, V, CacheEntry<K, V>> mapper) {
      this.innerFunction = innerFunction;
      this.mapper = mapper;
   }

   @ProtoFactory
   EntryFunctionEncoder(MarshallableObject<Function<Publisher<CacheEntry<K, V>>, O>> innerFunction, EncoderEntryMapper<K, V, CacheEntry<K, V>> mapper) {
      this(MarshallableObject.unwrap(innerFunction), mapper);
   }

   @ProtoField(2)
   MarshallableObject<Function<Publisher<CacheEntry<K, V>>, O>> getInnerFunction() {
      return MarshallableObject.create(innerFunction);
   }

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
}
