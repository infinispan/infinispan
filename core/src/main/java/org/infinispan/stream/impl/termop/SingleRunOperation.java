package org.infinispan.stream.impl.termop;

import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;

/**
 * Runs the provided function once only and returns the result.  This is useful for operations that can be performed
 * and its result is still valid even when a segment is lost.
 * @param <E> type of the output of the function
 * @param <R> type of the stream entries
 * @param <S> type of the stream itself
 */
public class SingleRunOperation<E, R, S extends BaseStream<R, S>> extends BaseTerminalOperation
        implements TerminalOperation<E> {
   private final Function<S, ? extends E> function;
   private transient AtomicBoolean complete;

   public SingleRunOperation(Iterable<IntermediateOperation> intermediateOperations,
                             Supplier<? extends BaseStream<?, ?>> supplier, Function<S, ? extends E> function) {
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
      E value = function.apply((S) stream);
      complete.set(true);
      return value;
   }

   public Function<S, ? extends E> getFunction() {
      return function;
   }
}
