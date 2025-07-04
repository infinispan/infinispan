package org.infinispan.stream.impl.intops.primitive.l;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs as double operation on a {@link LongStream}
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_PRIMITIVE_LONG_AS_DOUBLE_OPERATION)
public class AsDoubleLongOperation implements MappingOperation<Long, LongStream, Double, DoubleStream> {
   private static final AsDoubleLongOperation OPERATION = new AsDoubleLongOperation();
   private AsDoubleLongOperation() { }

   @ProtoFactory
   public static AsDoubleLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(LongStream stream) {
      return stream.asDoubleStream();
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Long> input) {
      return input.map(Long::doubleValue);
   }
}
