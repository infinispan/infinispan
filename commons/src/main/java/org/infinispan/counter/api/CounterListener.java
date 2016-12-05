package org.infinispan.counter.api;

/**
 * A listener interface to listen to {@link StrongCounter} changes.
 * <p>
 * The events received will have the previous/current value and its previous/current state.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CounterListener {

   void onUpdate(CounterEvent entry);
}
