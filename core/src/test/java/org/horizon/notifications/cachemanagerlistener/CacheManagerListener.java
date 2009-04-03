package org.horizon.notifications.cachemanagerlistener;

import org.horizon.notifications.Listener;
import org.horizon.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.horizon.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.horizon.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.horizon.notifications.cachemanagerlistener.event.Event;

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
