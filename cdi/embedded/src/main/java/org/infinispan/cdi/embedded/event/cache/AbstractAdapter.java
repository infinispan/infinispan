package org.infinispan.cdi.embedded.event.cache;

import jakarta.enterprise.event.Event;

/**
 * @author Pete Muir
 */
public abstract class AbstractAdapter<T extends org.infinispan.notifications.cachelistener.event.Event<?, ?>> {

   private final Event<T> event;

   public AbstractAdapter(Event<T> event) {
      this.event = event;
   }

   public void fire(T payload) {
      this.event.fire(payload);
   }
}
