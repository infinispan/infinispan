package org.infinispan.counter.api;

/**
 * A synchronous {@link WeakCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public interface SyncWeakCounter {


   /**
    * @see WeakCounter#getName()
    */
   String getName();

   /**
    * @see WeakCounter#getValue()
    */
   long getValue();

   /**
    * @see WeakCounter#increment()
    */
   default void increment() {
      add(1);
   }

   /**
    * @see WeakCounter#decrement()
    */
   default void decrement() {
      add(-1);
   }

   /**
    * @see WeakCounter#add(long)
    */
   void add(long delta);

   /**
    * @see WeakCounter#reset()
    */
   void reset();

   /**
    * @see WeakCounter#getConfiguration()
    */
   CounterConfiguration getConfiguration();

   /**
    * @see WeakCounter#remove()
    */
   void remove();

}
