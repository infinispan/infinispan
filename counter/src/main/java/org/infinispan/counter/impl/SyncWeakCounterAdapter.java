package org.infinispan.counter.impl;

import static org.infinispan.counter.impl.Util.awaitCounterOperation;

import java.util.Objects;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;

/**
 * A {@link WeakCounter} decorator that waits for the operation to complete.
 *
 * @author Pedro Ruivo
 * @see WeakCounter
 * @since 9.2
 */
public class SyncWeakCounterAdapter implements SyncWeakCounter {

   private final WeakCounter counter;

   public SyncWeakCounterAdapter(WeakCounter counter) {
      this.counter = Objects.requireNonNull(counter);
   }

   /**
    * @see WeakCounter#getName()
    */
   @Override
   public String getName() {
      return counter.getName();
   }

   /**
    * @see WeakCounter#getValue()
    */
   @Override
   public long getValue() {
      return counter.getValue();
   }

   /**
    * @see WeakCounter#add(long)
    */
   @Override
   public void add(long delta) {
      awaitCounterOperation(counter.add(delta));
   }

   /**
    * @see WeakCounter#reset()
    */
   @Override
   public void reset() {
      awaitCounterOperation(counter.reset());
   }

   /**
    * @see WeakCounter#getConfiguration()
    */
   @Override
   public CounterConfiguration getConfiguration() {
      return counter.getConfiguration();
   }

   /**
    * @see WeakCounter#remove()
    */
   @Override
   public void remove() {
      awaitCounterOperation(counter.remove());
   }

   @Override
   public String toString() {
      return "SyncWeakCounter{" +
             "counter=" + counter +
             '}';
   }
}
