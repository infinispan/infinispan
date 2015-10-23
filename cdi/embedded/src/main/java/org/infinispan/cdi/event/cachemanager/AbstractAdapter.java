package org.infinispan.cdi.event.cachemanager;

import javax.enterprise.event.Event;

/**
 * @author Pete Muir
 */
public abstract class AbstractAdapter<T extends org.infinispan.notifications.cachemanagerlistener.event.Event> {

   private final Event<T> event;

   public AbstractAdapter(Event<T> event) {
      this.event = event;
   }

   public void fire(T payload) {
      this.event.fire(payload);
   }
}
