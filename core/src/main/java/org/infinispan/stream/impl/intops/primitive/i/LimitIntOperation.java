package org.infinispan.stream.impl.intops.primitive.i;

import java.util.stream.IntStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs limit operation on a {@link IntStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_INT_LIMIT_OPERATION)
public class LimitIntOperation implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {

   @ProtoField(1)
   final long limit;

   @ProtoFactory
   public LimitIntOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.limit(limit);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.take(limit);
   }
}
