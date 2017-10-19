package org.infinispan.counter;

import java.util.Objects;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.util.Utils;

/**
 * A {@link StrongCounter} decorator that waits for the operation to complete.
 *
 * @author Pedro Ruivo
 * @see StrongCounter
 * @since 9.0
 */
public class SyncStrongCounter {

   private final StrongCounter counter;

   public SyncStrongCounter(StrongCounter counter) {
      this.counter = Objects.requireNonNull(counter);
   }

   /**
    * @see StrongCounter#incrementAndGet()
    */
   public long incrementAndGet() {
      return Utils.awaitCounterOperation(counter.incrementAndGet());
   }

   /**
    * @see StrongCounter#decrementAndGet()
    */
   public long decrementAndGet() {
      return Utils.awaitCounterOperation(counter.decrementAndGet());
   }

   /**
    * @see StrongCounter#addAndGet(long)
    */
   public long addAndGet(long delta) {
      return Utils.awaitCounterOperation(counter.addAndGet(delta));
   }

   /**
    * @see StrongCounter#reset()
    */
   public void reset() {
      Utils.awaitCounterOperation(counter.reset());
   }

   /**
    * @see StrongCounter#decrementAndGet()
    */
   public long getValue() {
      return Utils.awaitCounterOperation(counter.getValue());
   }

   /**
    * @see StrongCounter#compareAndSet(long, long)
    */
   public boolean compareAndSet(long expect, long update) {
      return Utils.awaitCounterOperation(counter.compareAndSet(expect, update));
   }

   /**
    * @see StrongCounter#getName()
    */
   public String getName() {
      return counter.getName();
   }

   /**
    * @see StrongCounter#getConfiguration()
    */
   public CounterConfiguration getConfiguration() {
      return counter.getConfiguration();
   }

   /**
    * @see StrongCounter#remove()
    */
   public void remove() {
      Utils.awaitCounterOperation(counter.remove());
   }

   @Override
   public String toString() {
      return "SyncStrongCounter{" +
            "counter=" + counter +
            '}';
   }
}
