package org.infinispan.stream.impl.intops.primitive.d;

import java.util.function.DoublePredicate;
import java.util.stream.DoubleStream;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs filter operation on a {@link DoubleStream}
 */
public class FilterDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private final DoublePredicate predicate;

   public FilterDoubleOperation(DoublePredicate predicate) {
      this.predicate = predicate;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.filter(predicate);
   }

   public DoublePredicate getPredicate() {
      return predicate;
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.filter(predicate::test);
   }
}
