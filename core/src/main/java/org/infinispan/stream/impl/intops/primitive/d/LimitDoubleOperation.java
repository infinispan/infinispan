package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs limit operation on a {@link DoubleStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_DOUBLE_LIMIT_OPERATION)
public class LimitDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {

   @ProtoField(1)
   final long limit;

   @ProtoFactory
   public LimitDoubleOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.limit(limit);
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.take(limit);
   }
}
