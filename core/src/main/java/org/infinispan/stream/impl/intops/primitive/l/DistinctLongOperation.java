package org.infinispan.stream.impl.intops.primitive.l;

import java.util.stream.LongStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs distinct operation on a {@link LongStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_LONG_DISTINCT_OPERATION)
public class DistinctLongOperation implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private static final DistinctLongOperation OPERATION = new DistinctLongOperation();
   private DistinctLongOperation() { }

   @ProtoFactory
   public static DistinctLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.distinct();
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.distinct();
   }
}
