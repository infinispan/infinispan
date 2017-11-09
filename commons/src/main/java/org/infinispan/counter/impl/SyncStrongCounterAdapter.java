package org.infinispan.counter.impl;

import static org.infinispan.counter.impl.Util.awaitCounterOperation;

import java.util.Objects;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.SyncStrongCounter;

/**
 * A {@link StrongCounter} decorator that waits for the operation to complete.
 *
 * @author Pedro Ruivo
 * @see StrongCounter
 * @since 9.2
 */
public class SyncStrongCounterAdapter implements SyncStrongCounter {

   private final StrongCounter counter;

   public SyncStrongCounterAdapter(StrongCounter counter) {
      this.counter = Objects.requireNonNull(counter);
   }

   /**
    * @see StrongCounter#addAndGet(long)
    */
   @Override
   public long addAndGet(long delta) {
      return awaitCounterOperation(counter.addAndGet(delta));
   }

   /**
    * @see StrongCounter#reset()
    */
   @Override
   public void reset() {
      awaitCounterOperation(counter.reset());
   }

   /**
    * @see StrongCounter#decrementAndGet()
    */
   @Override
   public long getValue() {
      return awaitCounterOperation(counter.getValue());
   }

   @Override
   public long compareAndSwap(long expect, long update) {
      return awaitCounterOperation(counter.compareAndSwap(expect, update));
   }

   /**
    * @see StrongCounter#getName()
    */
   @Override
   public String getName() {
      return counter.getName();
   }

   /**
    * @see StrongCounter#getConfiguration()
    */
   @Override
   public CounterConfiguration getConfiguration() {
      return counter.getConfiguration();
   }

   /**
    * @see StrongCounter#remove()
    */
   @Override
   public void remove() {
      awaitCounterOperation(counter.remove());
   }

   @Override
   public String toString() {
      return "SyncStrongCounter{" +
             "counter=" + counter +
             '}';
   }
}
