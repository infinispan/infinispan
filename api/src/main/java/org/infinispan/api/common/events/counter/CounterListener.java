package org.infinispan.api.common.events.counter;

import org.infinispan.api.common.events.counter.CounterEvent;

/**
 * A listener interface to listen to counter changes.
 * <p>
 * The events received will have the previous/current value and its previous/current state.
 *
 * @since 14.0
 */
public interface CounterListener {

   void onUpdate(CounterEvent entry);
}
