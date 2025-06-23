package org.infinispan.stream.impl.intops.primitive.l;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs boxed operation on a {@link LongStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_LONG_BOXED_OPERATION)
public class BoxedLongOperation implements MappingOperation<Long, LongStream, Long, Stream<Long>> {
   private static final BoxedLongOperation OPERATION = new BoxedLongOperation();
   private BoxedLongOperation() { }

   @ProtoFactory
   public static BoxedLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public Stream<Long> perform(LongStream stream) {
      return stream.boxed();
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<Long> input) {
      return input;
   }
}
