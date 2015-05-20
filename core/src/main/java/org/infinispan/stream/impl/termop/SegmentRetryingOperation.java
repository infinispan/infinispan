package org.infinispan.stream.impl.termop;

import org.infinispan.stream.impl.TerminalOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

/**
 * A terminal based operation that runs the provided function to evaluate the operation.  If a segment is lost during
 * the evaluation of the function the function results will be ignored and subsequently retried with the new stable
 * segments.  This is repeated until either a full stable run is completed of the function or if the lost segment
 * states that there are no more segments left.
 * @param <E> output type of the function
 * @param <T> type of the stream entries
 * @param <S> type of the stream itself
 */
public class SegmentRetryingOperation<E, T, S extends BaseStream<T, S>> extends BaseTerminalOperation
        implements TerminalOperation<E> {
   private static final Log log = LogFactory.getLog(SegmentRetryingOperation.class);

   private static final BaseStream<?, ?> EMPTY = Stream.empty();

   private final Function<S, ? extends E> function;
   private transient AtomicReference<BaseStream<?, ?>> streamRef = new AtomicReference<>(EMPTY);
   private transient AtomicBoolean continueTrying = new AtomicBoolean(true);

   public SegmentRetryingOperation(Iterable<IntermediateOperation> intermediateOperations,
           Supplier<? extends Stream<?>> supplier, Function<S, ? extends E> function) {
      super(intermediateOperations, supplier);
      this.function = function;
   }

   @Override
   public boolean lostSegment(boolean stopIfLost) {
      BaseStream<?, ?> oldStream = streamRef.get();
      continueTrying.set(!stopIfLost);
      boolean affected;
      if (oldStream != null) {
         // If the stream was non null and wasn't empty that means we were processing it at the time of the segment
         // being lost - so we tell that one to close
         if (oldStream != EMPTY) {
            // This can only fail if the operation completes concurrently
            if ((affected = streamRef.compareAndSet(oldStream, EMPTY))) {
               // This can short circuit some things like sending a response or waiting for retrieval from a
               // cache loader
               oldStream.close();
            }
         } else {
            affected = true;
         }
      } else {
         affected = false;
      }
      return affected;
   }

   private E innerPerformOperation(BaseStream<?, ?> stream) {
      for (IntermediateOperation intOp : intermediateOperations) {
         stream = intOp.perform(stream);
      }
      return function.apply((S) stream);
   }

   @Override
   public E performOperation() {
      boolean keepTrying = true;
      BaseStream<?, ?> stream;
      E value;
      do {
         stream = supplier.get();
         streamRef.set(stream);
         value = innerPerformOperation(stream);
         log.trace("Completed an operation, trying to see if we are done.");
      } while (!streamRef.compareAndSet(stream, null) && (keepTrying = continueTrying.get()));
      log.tracef("Operation now done, due to try denial: " + !keepTrying);
      return keepTrying ? value : null;
   }

   public Function<S, ? extends E> getFunction() {
      return function;
   }
}
