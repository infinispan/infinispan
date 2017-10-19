package org.infinispan.counter.util;

import org.infinispan.counter.SyncWeakCounter;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.WeakCounter;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public class WeakTestCounter implements TestCounter {

   private final SyncWeakCounter syncCounter;
   private final WeakCounter counter;

   public WeakTestCounter(WeakCounter counter) {
      this.counter = counter;
      this.syncCounter = new SyncWeakCounter(counter);
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return counter.addListener(listener);
   }

   @Override
   public void increment() {
      syncCounter.increment();
   }

   @Override
   public void add(long delta) {
      syncCounter.add(delta);
   }

   @Override
   public void decrement() {
      syncCounter.decrement();
   }

   @Override
   public long getValue() {
      return syncCounter.getValue();
   }

   @Override
   public void reset() {
      syncCounter.reset();
   }

   @Override
   public String getName() {
      return syncCounter.getName();
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return syncCounter.getConfiguration();
   }

   @Override
   public boolean isSame(TestCounter other) {
      return other instanceof WeakTestCounter && counter == ((WeakTestCounter) other).counter;
   }

   @Override
   public void remove() {
      syncCounter.remove();
   }
}
