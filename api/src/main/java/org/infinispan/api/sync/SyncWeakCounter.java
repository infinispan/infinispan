package org.infinispan.api.sync;

/**
 * @since 14.0
 **/
public interface SyncWeakCounter {
   /**
    * Returns the name of this counter
    *
    * @return the name of this counter
    */
   String name();

   /**
    * Return the container of this cache
    * @return
    */
   SyncContainer container();

   long value();

   /**
    * Increments the counter.
    */
   default void increment() {
      add(1L);
   }
   
   /**
    * Decrements the counter.
    */
   default void decrement() {
      add(-1L);
   }

   /**
    * Adds the given value to the new value.
    *
    * @param delta the value to add.
    */
   void add(long delta);

   /**
    * Resets the counter to its initial value.
    */
   void reset();
}
