package org.infinispan.stream.impl.intops.object;

import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.stream.impl.intops.FlatMappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs flat map to long operation on a regular {@link Stream}
 * @param <I> the type of the input stream
 */
@ProtoTypeId(ProtoStreamTypeIds.STREAM_INTOP_FLATMAP_TO_LONG_OPERATION)
public class FlatMapToLongOperation<I> implements FlatMappingOperation<I, Stream<I>, Long, LongStream> {
   private final Function<? super I, ? extends LongStream> function;

   public FlatMapToLongOperation(Function<? super I, ? extends LongStream> function) {
      this.function = function;
   }

   @ProtoFactory
   FlatMapToLongOperation(MarshallableObject<Function<? super I, ? extends LongStream>> function) {
      this.function = MarshallableObject.unwrap(function);
   }

   @ProtoField(1)
   MarshallableObject<Function<? super I, ? extends LongStream>> getFunction() {
      return MarshallableObject.create(function);
   }

   @Override
   public LongStream perform(Stream<I> stream) {
      return stream.flatMapToLong(function);
   }

   @Override
   public Stream<LongStream> map(Stream<I> iStream) {
      return iStream.map(function);
   }

   @Override
   public Flowable<Long> mapFlowable(Flowable<I> input) {
      return input.concatMapStream(o -> function.apply(o).boxed());
   }
}
