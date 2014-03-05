package org.infinispan.notifications.impl;

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
    */
   void invoke(T event);

   /**
    * The listener instance that is notified of events
    */
   Object getTarget();
}
