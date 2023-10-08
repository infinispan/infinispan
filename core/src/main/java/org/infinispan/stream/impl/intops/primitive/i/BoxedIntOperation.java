package org.infinispan.stream.impl.intops.primitive.i;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs boxed operation on a {@link IntStream}
 */
public class BoxedIntOperation implements MappingOperation<Integer, IntStream, Integer, Stream<Integer>> {
   private static final BoxedIntOperation OPERATION = new BoxedIntOperation();
   private BoxedIntOperation() { }

   @ProtoFactory
   public static BoxedIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public Stream<Integer> perform(IntStream stream) {
      return stream.boxed();
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input;
   }
}
