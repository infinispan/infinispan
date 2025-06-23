package org.infinispan.stream.impl.intops.primitive.l;

import java.util.function.LongPredicate;
import java.util.stream.LongStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs filter operation on a {@link LongStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_LONG_FILTER_OPERATION)
public class FilterLongOperation<S> implements IntermediateOperation<Long, LongStream, Long, LongStream> {
   private final LongPredicate predicate;

   public FilterLongOperation(LongPredicate predicate) {
      this.predicate = predicate;
   }

   @ProtoFactory
   FilterLongOperation(MarshallableObject<LongPredicate> predicate) {
      this.predicate = MarshallableObject.unwrap(predicate);
   }

   @ProtoField(1)
   MarshallableObject<LongPredicate> getPredicate() {
      return MarshallableObject.create(predicate);
   }

   @Override
   public LongStream perform(LongStream stream) {
      return stream.filter(predicate);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input.filter(predicate::test);
   }
}
