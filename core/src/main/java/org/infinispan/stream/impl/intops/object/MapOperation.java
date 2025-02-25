package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.cache.impl.EncodingFunction;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs map to operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 * @param <O> the type of the output stream
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_MAP_OPERATION)
public class MapOperation<I, O> implements MappingOperation<I, Stream<I>, O, Stream<O>> {
   private final Function<? super I, ? extends O> function;

   public MapOperation(Function<? super I, ? extends O> function) {
      this.function = function;
   }

   @ProtoFactory
   MapOperation(MarshallableObject<Function<? super I, ? extends O>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<Function<? super I, ? extends O>> getFunction() {
      return MarshallableObject.create(function);
   }

   public boolean isEncodingFunction() {
      return function instanceof EncodingFunction;
   }

   @Override
   public Stream<O> perform(Stream<I> stream) {
      return stream.map(function);
   }

   @Override
   public void handleInjection(ComponentRegistry registry) {
      registry.wireDependencies(function);
   }

   @Override
   public Flowable<O> mapFlowable(Flowable<I> input) {
      return input.map(function::apply);
   }
}
