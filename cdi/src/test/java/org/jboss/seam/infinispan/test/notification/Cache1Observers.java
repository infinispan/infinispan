package org.jboss.seam.infinispan.test.notification;

import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
public class Cache1Observers {

   private CacheStartedEvent cacheStartedEvent;
   private int cacheStartedEventCount;

   private int cacheStoppedEventCount;
   private CacheStoppedEvent cacheStoppedEvent;

   private int cacheEntryCreatedEventCount;
   private CacheEntryCreatedEvent cacheEntryCreatedEvent;

   private int cacheEntryRemovedEventCount;
   private CacheEntryRemovedEvent cacheEntryRemovedEvent;

   /**
    * Observe the cache started event for the cache associated with @Cache1
    */
   void observeCacheStarted(@Observes @Cache1 CacheStartedEvent event) {
      this.cacheStartedEventCount++;
      this.cacheStartedEvent = event;
   }

   /**
    * Observe the cache stopped event for the cache associated with @Cache1
    */
   void observeCacheStopped(@Observes @Cache1 CacheStoppedEvent event) {
      this.cacheStoppedEventCount++;
      this.cacheStoppedEvent = event;
   }

   /**
    * Observe the cache entry created event for the cache associated with @Cache1
    * <p/>
    * Get's called once (before) with pre false, once (after) with pre true
    */
   void observeCacheEntryCreated(@Observes @Cache1 CacheEntryCreatedEvent event) {
      if (!event.isPre()) {
         this.cacheEntryCreatedEventCount++;
         this.cacheEntryCreatedEvent = event;
      }
   }

   /**
    * Observe the cache entry removed event for the cache associated with @Cache1
    */
   void observeCacheEntryRemoved(@Observes @Cache1 CacheEntryRemovedEvent event) {
      if (event.isPre()) {
         this.cacheEntryRemovedEventCount++;
         this.cacheEntryRemovedEvent = event;
      }
   }

   public CacheStartedEvent getCacheStartedEvent() {
      return cacheStartedEvent;
   }

   public int getCacheStartedEventCount() {
      return cacheStartedEventCount;
   }

   public CacheStoppedEvent getCacheStoppedEvent() {
      return cacheStoppedEvent;
   }

   public int getCacheStoppedEventCount() {
      return cacheStoppedEventCount;
   }

   public CacheEntryCreatedEvent getCacheEntryCreatedEvent() {
      return cacheEntryCreatedEvent;
   }

   public int getCacheEntryCreatedEventCount() {
      return cacheEntryCreatedEventCount;
   }

   public CacheEntryRemovedEvent getCacheEntryRemovedEvent() {
      return cacheEntryRemovedEvent;
   }

   public int getCacheEntryRemovedEventCount() {
      return cacheEntryRemovedEventCount;
   }

}
