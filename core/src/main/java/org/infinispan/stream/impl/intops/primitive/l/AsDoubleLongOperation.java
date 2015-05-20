package org.infinispan.stream.impl.intops.primitive.l;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 * Performs as double operation on a {@link LongStream}
 */
public class AsDoubleLongOperation implements IntermediateOperation<Long, LongStream, Double, DoubleStream> {
   private static final AsDoubleLongOperation OPERATION = new AsDoubleLongOperation();
   private AsDoubleLongOperation() { }

   public static AsDoubleLongOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(LongStream stream) {
      return stream.asDoubleStream();
   }
}
