package org.infinispan.api.common.events.counter;

import org.infinispan.api.common.events.CounterState;

/**
 * The event used by {@link CounterListener}.
 *
 * @since 14.0
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
