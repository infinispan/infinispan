package org.infinispan.stream.impl.intops.primitive.l;

import java.util.stream.LongStream;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs limit operation on a {@link LongStream}
 */
public class LimitLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   @ProtoField(number = 1, defaultValue = "-1")
   final long limit;

   @ProtoFactory
   public LimitLongOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.limit(limit);
   }

   public long getLimit() {
      return limit;
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.take(limit);
   }
}
