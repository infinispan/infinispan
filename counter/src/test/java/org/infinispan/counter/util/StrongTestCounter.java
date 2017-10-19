package org.infinispan.counter.util;

import org.infinispan.counter.SyncStrongCounter;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.StrongCounter;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public class StrongTestCounter implements TestCounter {

   private final SyncStrongCounter syncCounter;
   private final StrongCounter counter;

   public StrongTestCounter(StrongCounter counter) {
      this.counter = counter;
      this.syncCounter = new SyncStrongCounter(counter);
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return counter.addListener(listener);
   }

   @Override
   public void increment() {
      syncCounter.incrementAndGet();
   }

   @Override
   public void add(long delta) {
      syncCounter.addAndGet(delta);
   }

   public long addAndGet(long delta) {
      return syncCounter.addAndGet(delta);
   }

   @Override
   public void decrement() {
      syncCounter.decrementAndGet();
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
      return other instanceof StrongTestCounter && counter == ((StrongTestCounter) other).counter;
   }

   @Override
   public void remove() {
      syncCounter.remove();
   }

   public boolean compareAndSet(long expect, long value) {
      return syncCounter.compareAndSet(expect, value);
   }
}
