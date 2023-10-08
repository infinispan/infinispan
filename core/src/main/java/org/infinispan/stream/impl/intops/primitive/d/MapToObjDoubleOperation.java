package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs boxed operation on a {@link DoubleStream}
 * @param <R> the type of the output stream
 */
public class MapToObjDoubleOperation<R> implements MappingOperation<Double, DoubleStream, R, Stream<R>> {
   private final DoubleFunction<? extends R> function;

   public MapToObjDoubleOperation(DoubleFunction<? extends R> function) {
      this.function = function;
   }

   @ProtoFactory
   MapToObjDoubleOperation(MarshallableObject<DoubleFunction<? extends R>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<DoubleFunction<? extends R>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public Stream<R> perform(DoubleStream stream) {
      return stream.mapToObj(function);
   }

   @Override
   public Flowable<R> mapFlowable(Flowable<Double> input) {
      return input.map(function::apply);
   }
}
