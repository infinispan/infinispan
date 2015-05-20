package org.infinispan.stream.impl.intops.primitive.i;

import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Performs boxed operation on a {@link IntStream}
 */
public class BoxedIntOperation implements IntermediateOperation<Integer, IntStream, Integer, Stream<Integer>> {
   private static final BoxedIntOperation OPERATION = new BoxedIntOperation();
   private BoxedIntOperation() { }

   public static BoxedIntOperation getInstance() {
      return OPERATION;
   }

   @Override
   public Stream<Integer> perform(IntStream stream) {
      return stream.boxed();
   }
}
