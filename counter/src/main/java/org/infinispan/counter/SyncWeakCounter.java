package org.infinispan.counter;

import java.util.Objects;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.util.Utils;

/**
 * A {@link WeakCounter} decorator that waits for the operation to complete.
 *
 * @author Pedro Ruivo
 * @see WeakCounter
 * @since 9.0
 */
public class SyncWeakCounter {

   private final WeakCounter counter;

   public SyncWeakCounter(WeakCounter counter) {
      this.counter = Objects.requireNonNull(counter);
   }

   /**
    * @see WeakCounter#getName()
    */
   public String getName() {
      return counter.getName();
   }

   /**
    * @see WeakCounter#getValue()
    */
   public long getValue() {
      return counter.getValue();
   }

   /**
    * @see WeakCounter#increment()
    */
   public void increment() {
      Utils.awaitCounterOperation(counter.increment());
   }

   /**
    * @see WeakCounter#decrement()
    */
   public void decrement() {
      Utils.awaitCounterOperation(counter.decrement());
   }

   /**
    * @see WeakCounter#add(long)
    */
   public void add(long delta) {
      Utils.awaitCounterOperation(counter.add(delta));
   }

   /**
    * @see WeakCounter#reset()
    */
   public void reset() {
      Utils.awaitCounterOperation(counter.reset());
   }

   /**
    * @see WeakCounter#getConfiguration()
    */
   public CounterConfiguration getConfiguration() {
      return counter.getConfiguration();
   }

   /**
    * @see WeakCounter#remove()
    */
   public void remove() {
      Utils.awaitCounterOperation(counter.remove());
   }

   @Override
   public String toString() {
      return "SyncWeakCounter{" +
            "counter=" + counter +
            '}';
   }
}
