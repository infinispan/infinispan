package org.infinispan.counter.api;

import java.util.concurrent.CompletableFuture;

/**
 * A weak consistent counter interface.
 * <p>
 * This interface represents a weak counter in the way that the write operations does not return a consistent results.
 * In this way, all the writes return a {@link CompletableFuture<Void>}.
 * <p>
 * Note: the reset operation is not atomic.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface WeakCounter {

   /**
    * @return The counter name.
    */
   String getName();

   /**
    * It returns the counter's value.
    * <p>
    * This value may be not the mot up-to-data value.
    *
    * @return The counter's value.
    */
   long getValue();

   /**
    * Increments the counter.
    */
   default CompletableFuture<Void> increment() {
      return add(1L);
   }


   /**
    * Decrements the counter.
    */
   default CompletableFuture<Void> decrement() {
      return add(-1L);
   }


   /**
    * Adds the given value to the new value.
    *
    * @param delta the value to add.
    */
   CompletableFuture<Void> add(long delta);

   /**
    * Resets the counter to its initial value.
    */
   CompletableFuture<Void> reset();

   /**
    * Adds a {@link CounterListener} to this counter.
    *
    * @param listener The listener to add.
    * @param <T>      The type of the listener. It must implement {@link CounterListener}.
    * @return A {@link Handle} that allows to remove the listener via {@link Handle#remove()} ()}.
    */
   <T extends CounterListener> Handle<T> addListener(T listener);

   /**
    * @return the {@link CounterConfiguration} used by this counter.
    */
   CounterConfiguration getConfiguration();

   /**
    * It removes this counter from the cluster.
    * <p>
    * Note that it doesn't remove the counter from the {@link CounterManager}. If you want to remove the counter from
    * the {@link CounterManager} use {@link CounterManager#remove(String)}.
    *
    * @return The {@link CompletableFuture} that is completed when the counter is removed from the cluster.
    */
   CompletableFuture<Void> remove();

   /**
    * It returns a synchronous weak counter for this instance.
    *
    * @return a {@link SyncWeakCounter}.
    */
   SyncWeakCounter sync();
}
