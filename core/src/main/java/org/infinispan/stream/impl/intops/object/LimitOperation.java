package org.infinispan.stream.impl.intops.object;

import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs limit operation on a regular {@link Stream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_LIMIT_OPERATION)
public class LimitOperation<S> implements IntermediateOperation<S, Stream<S>, S, Stream<S>> {

   @ProtoField(1)
   final long limit;

   @ProtoFactory
   public LimitOperation(long limit) {
      if (limit <= 0) {
         throw new IllegalArgumentException("Limit must be greater than 0");
      }
      this.limit = limit;
   }

   @Override
   public Stream<S> perform(Stream<S> stream) {
      return stream.limit(limit);
   }

   @Override
   public Flowable<S> mapFlowable(Flowable<S> input) {
      return input.take(limit);
   }
}
