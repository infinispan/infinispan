package org.infinispan.stream.impl.intops.primitive.i;

import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.stream.impl.intops.MappingOperation;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Performs as double operation on a {@link IntStream}
 */
public class AsDoubleIntOperation implements MappingOperation<Integer, IntStream, Double, DoubleStream> {
   private static final AsDoubleIntOperation OPERATION = new AsDoubleIntOperation();
   private AsDoubleIntOperation() { }

   @ProtoFactory
   public static AsDoubleIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(IntStream stream) {
      return stream.asDoubleStream();
   }

   @Override
   public Flowable<Double> mapFlowable(Flowable<Integer> input) {
      return input.map(Integer::doubleValue);
   }
}
