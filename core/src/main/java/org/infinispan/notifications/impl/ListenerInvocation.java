package org.infinispan.notifications.impl;

import java.util.concurrent.CompletionStage;

/**
 * Defines simple listener invocation.
 *
 * @param <T> The type of event to listen to
 * @author wburns
 * @since 7.0
 */
public interface ListenerInvocation<T> {
   /**
    * Invokes the event
    * @param event
    * @return null if event was ignored or already complete otherwise the event will be completely notified when
    * the provided stage is completed
    */
   CompletionStage<Void> invoke(T event);

   /**
    * The listener instance that is notified of events
    */
   Object getTarget();
}
