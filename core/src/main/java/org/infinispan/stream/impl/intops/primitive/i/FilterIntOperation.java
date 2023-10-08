package org.infinispan.stream.impl.intops.primitive.i;

import java.util.function.IntPredicate;
import java.util.stream.IntStream;

import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs filter operation on a {@link IntStream}
 */
public class FilterIntOperation<S> implements IntermediateOperation<Integer, IntStream, Integer, IntStream> {
   private final IntPredicate predicate;

   public FilterIntOperation(IntPredicate predicate) {
      this.predicate = predicate;
   }

   @ProtoFactory
   FilterIntOperation(MarshallableObject<IntPredicate> predicate) {
      this.predicate = MarshallableObject.unwrap(predicate);
   }

   @ProtoField(1)
   MarshallableObject<IntPredicate> getPredicate() {
      return MarshallableObject.create(predicate);
   }

   @Override
   public IntStream perform(IntStream stream) {
      return stream.filter(predicate);
   }

   @Override
   public Flowable<Integer> mapFlowable(Flowable<Integer> input) {
      return input.filter(predicate::test);
   }
}
