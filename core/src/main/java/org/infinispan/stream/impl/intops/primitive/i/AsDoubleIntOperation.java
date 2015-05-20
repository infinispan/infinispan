package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

/**
 * Performs as double operation on a {@link IntStream}
 */
public class AsDoubleIntOperation implements IntermediateOperation<Integer, IntStream, Double, DoubleStream> {
   private static final AsDoubleIntOperation OPERATION = new AsDoubleIntOperation();
   private AsDoubleIntOperation() { }

   public static AsDoubleIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(IntStream stream) {
      return stream.asDoubleStream();
   }
}
