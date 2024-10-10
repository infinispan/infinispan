package org.infinispan.counter.api;

/**
 * The event used by {@link CounterListener}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CounterEvent {

   /**
    * @return the previous value.
    */
   long getOldValue();

   /**
    * @return the previous state.
    */
   CounterState getOldState();

   /**
    * @return the counter value.
    */
   long getNewValue();

   /**
    * @return the counter state.
    */
   CounterState getNewState();

}
