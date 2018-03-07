package org.infinispan.stream.impl.termop;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;

/**
 * Runs the provided function once only and returns the result.  This is useful for operations that can be performed
 * and its result is still valid even when a segment is lost.
 * @param <Original> original stream type
 * @param <E> type of the output of the function
 * @param <R> type of the stream entries
 * @param <S> type of the stream itself
 */
public class SingleRunOperation<Original, E, R, S extends BaseStream<R, S>, S2 extends S> extends BaseTerminalOperation<Original>
        implements TerminalOperation<Original, E> {
   private final Function<? super S2, ? extends E> function;
   private transient AtomicBoolean complete;

   public SingleRunOperation(Iterable<IntermediateOperation> intermediateOperations,
                             Supplier<Stream<Original>> supplier, Function<? super S2, ? extends E> function) {
      super(intermediateOperations, supplier);
      this.function = function;
      this.complete = new AtomicBoolean();
   }

   @Override
   public boolean lostSegment(boolean stopIfLost) {
      return !complete.get();
   }

   @Override
   public E performOperation() {
      BaseStream<?, ?> stream = supplier.get();
      for (IntermediateOperation intOp : intermediateOperations) {
         stream = intOp.perform(stream);
      }
      E value = function.apply((S2) stream);
      complete.set(true);
      return value;
   }

   public Function<? super S2, ? extends E> getFunction() {
      return function;
   }
}
