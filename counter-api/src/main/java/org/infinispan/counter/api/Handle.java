package org.infinispan.counter.api;

/**
 * As a return of {@link StrongCounter#addListener(CounterListener)}, it is used to un-register the {@link CounterListener}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface Handle<T extends CounterListener> {

   /**
    * @return the {@link CounterListener} managed by this.
    */
   T getCounterListener();

   /**
    * Removes the {@link CounterListener}.
    */
   void remove();
}
