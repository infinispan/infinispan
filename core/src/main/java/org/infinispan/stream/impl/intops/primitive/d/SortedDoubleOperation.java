package org.infinispan.stream.impl.intops.primitive.d;

import java.util.stream.DoubleStream;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs sorted operation on a {@link DoubleStream}
 */
public class SortedDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private static final SortedDoubleOperation OPERATION = new SortedDoubleOperation();
   private SortedDoubleOperation() { }

   @ProtoFactory
   public static SortedDoubleOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.sorted();
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Double> input) {
      return input.sorted();
   }
}
