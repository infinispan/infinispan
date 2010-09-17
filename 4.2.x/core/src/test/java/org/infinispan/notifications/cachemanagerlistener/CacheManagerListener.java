package org.infinispan.notifications.cachemanagerlistener;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.Event;

@Listener
public class CacheManagerListener {
   Event event;
   int invocationCount;

   public void reset() {
      event = null;
      invocationCount = 0;
   }

   public Event getEvent() {
      return event;
   }

   public int getInvocationCount() {
      return invocationCount;
   }

   // handler
   @CacheStarted
   @CacheStopped
   @ViewChanged
   public void handle(Event e) {
      event = e;
      invocationCount++;
   }
}
