package org.infinispan.stream.impl.intops.primitive.d;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.DoubleStream;

 /**
 * Performs distinct operation on a {@link DoubleStream}
 */
public class DistinctDoubleOperation implements IntermediateOperation<Double, DoubleStream, Double, DoubleStream> {
   private static final DistinctDoubleOperation OPERATION = new DistinctDoubleOperation();
   private DistinctDoubleOperation() { }

   public static DistinctDoubleOperation getInstance() {
      return OPERATION;
   }

   @Override
   public DoubleStream perform(DoubleStream stream) {
      return stream.distinct();
   }
}
