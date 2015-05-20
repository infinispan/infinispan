package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.DoubleStream;

/**
 * Performs sorted operation on a {@link DoubleStream}
 */
public class SortedDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private static final SortedDoubleOperation OPERATION = new SortedDoubleOperation();
   private SortedDoubleOperation() { }

   public static SortedDoubleOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.sorted();
   }
}
