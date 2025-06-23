package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs boxed operation on a {@link DoubleStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_DOUBLE_BOXED_OPERATION)
public class BoxedDoubleOperation implements MappingOperation<Double, DoubleStream, Double, Stream<Double>> {
   private static final BoxedDoubleOperation OPERATION = new BoxedDoubleOperation();
   private BoxedDoubleOperation() { }

   @ProtoFactory
   public static BoxedDoubleOperation getInstance() {
      return OPERATION;
   }

   @Override
   public Stream<Double> perform(DoubleStream stream) {
      return stream.boxed();
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input;
   }
}
