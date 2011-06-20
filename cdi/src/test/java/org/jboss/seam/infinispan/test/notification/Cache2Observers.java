package org.jboss.seam.infinispan.test.notification;

import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
public class Cache2Observers {

   private CacheStartedEvent cacheStartedEvent;
   private int cacheStartedEventCount;

   /**
    * Observe the cache started event for the cache associated with @Cache2
    */
   public void observeCacheStarted(@Observes @Cache2 CacheStartedEvent event) {
      this.cacheStartedEventCount++;
      this.cacheStartedEvent = event;
   }

   public CacheStartedEvent getCacheStartedEvent() {
      return cacheStartedEvent;
   }

   public int getCacheStartedEventCount() {
      return cacheStartedEventCount;
   }

}
