package org.infinispan.cdi.embedded.test.cachemanager.programmatic;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;

/**
 * @author Pete Muir
 */
@ApplicationScoped
public class SmallCacheObservers {

   private CacheStartedEvent cacheStartedEvent;
   private int cacheStartedEventCount;

   /**
    * Observe the cache started event for the cache associated with @Cache1
    */
   void observeCacheStarted(@Observes @Small CacheStartedEvent event) {
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
