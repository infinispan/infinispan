package org.infinispan.cdi.test.event;

import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

/**
 * {@link org.infinispan.cdi.test.event.Cache1} and {@link org.infinispan.cdi.test.event.Cache2} events observer.
 *
 * @author Pete Muir
 * @author Sebastian Laskawiec
 * @see org.infinispan.cdi.test.event.Cache1
 * @see org.infinispan.cdi.test.event.Cache2
 */
@ApplicationScoped
public class CacheObserver {

   private CacheEventHolder eventsMap = new CacheEventHolder();

   private void observeCache1CacheStatedEvent(@Observes @Cache1 CacheStartedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheStartedEvent.class, event);
   }

   private void observeCache2CacheStatedEvent(@Observes @Cache2 CacheStartedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheStartedEvent.class, event);
   }

   private void observeCache1CacheEntryCreatedEvent(@Observes @Cache1 CacheEntryCreatedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryCreatedEvent.class, event);
   }

   private void observeCache2CacheEntryCreatedEvent(@Observes @Cache2 CacheEntryCreatedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryCreatedEvent.class, event);
   }

   private void observeCache1CacheEntryRemovedEvent(@Observes @Cache1 CacheEntryRemovedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryRemovedEvent.class, event);
   }

   private void observeCache2CacheEntryRemovedEvent(@Observes @Cache2 CacheEntryRemovedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryRemovedEvent.class, event);
   }

   private void observeCache1CacheEntryActivatedEvent(@Observes @Cache1 CacheEntryActivatedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryActivatedEvent.class, event);
   }

   private void observeCache2CacheEntryActivatedEvent(@Observes @Cache2 CacheEntryActivatedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryActivatedEvent.class, event);
   }

   private void observeCache1CacheEntriesEvictedEvent(@Observes @Cache1 CacheEntriesEvictedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntriesEvictedEvent.class, event);
   }

   private void observeCache2CacheEntriesEvictedEvent(@Observes @Cache2 CacheEntriesEvictedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntriesEvictedEvent.class, event);
   }

   private void observeCache1CacheEntryModifiedEvent(@Observes @Cache1 CacheEntryModifiedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryModifiedEvent.class, event);
   }

   private void observeCache2CacheEntryModifiedEvent(@Observes @Cache2 CacheEntryModifiedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryModifiedEvent.class, event);
   }

   private void observeCache1CacheEntryInvalidatedEvent(@Observes @Cache1 CacheEntryInvalidatedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryInvalidatedEvent.class, event);
   }

   private void observeCache2CacheEntryInvalidatedEvent(@Observes @Cache2 CacheEntryInvalidatedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryInvalidatedEvent.class, event);
   }

   private void observeCache1CacheEntryLoadedEvent(@Observes @Cache1 CacheEntryLoadedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryLoadedEvent.class, event);
   }

   private void observeCache2CacheEntryLoadedEvent(@Observes @Cache2 CacheEntryLoadedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryLoadedEvent.class, event);
   }

   private void observeCache1CacheEntryPassivatedEvent(@Observes @Cache1 CacheEntryPassivatedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryPassivatedEvent.class, event);
   }

   private void observeCache2CacheEntryPassivatedEvent(@Observes @Cache2 CacheEntryPassivatedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryPassivatedEvent.class, event);
   }

   private void observeCache1CacheEntryVisitedEvent(@Observes @Cache1 CacheEntryVisitedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryVisitedEvent.class, event);
   }

   private void observeCache2CacheEntryVisitedEvent(@Observes @Cache2 CacheEntryVisitedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryVisitedEvent.class, event);
   }

   private void observeCache1DataRehashEvent(@Observes @Cache1 DataRehashedEvent event) {
      eventsMap.addEvent(Cache1.class, DataRehashedEvent.class, event);
   }

   private void observeCache2DataRehashEvent(@Observes @Cache2 DataRehashedEvent event) {
      eventsMap.addEvent(Cache1.class, DataRehashedEvent.class, event);
   }

   private void observeCache1CacheStoppedEvent(@Observes @Cache1 CacheStoppedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheStoppedEvent.class, event);
   }

   private void observeCache2CacheStoppedEvent(@Observes @Cache2 CacheStoppedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheStoppedEvent.class, event);
   }

   private void observeCache1TransactionCompletedEvent(@Observes @Cache1 TransactionCompletedEvent event) {
      eventsMap.addEvent(Cache1.class, TransactionCompletedEvent.class, event);
   }

   private void observeCache2TransactionCompletedEvent(@Observes @Cache2 TransactionCompletedEvent event) {
      eventsMap.addEvent(Cache2.class, TransactionCompletedEvent.class, event);
   }

   private void observeCache1TransactionRegisteredEvent(@Observes @Cache1 TransactionRegisteredEvent event) {
      eventsMap.addEvent(Cache1.class, TransactionRegisteredEvent.class, event);
   }

   private void observeCache2TransactionRegisteredEvent(@Observes @Cache2 TransactionRegisteredEvent event) {
      eventsMap.addEvent(Cache2.class, TransactionRegisteredEvent.class, event);
   }

   private void observeCache1ViewChangedEvent(@Observes @Cache1 ViewChangedEvent event) {
      eventsMap.addEvent(Cache1.class, ViewChangedEvent.class, event);
   }

   private void observeCache2ViewChangedEvent(@Observes @Cache2 ViewChangedEvent event) {
      eventsMap.addEvent(Cache2.class, ViewChangedEvent.class, event);
   }

   private void observeCache1TopologyChangedEvent(@Observes @Cache1 TopologyChangedEvent event) {
      eventsMap.addEvent(Cache1.class, TopologyChangedEvent.class, event);
   }

   private void observeCache2TopologyChangedEvent(@Observes @Cache2 TopologyChangedEvent event) {
      eventsMap.addEvent(Cache2.class, TopologyChangedEvent.class, event);
   }

   private void observeCache1CacheEntryEvictedEvent(@Observes @Cache1 CacheEntryEvictedEvent event) {
      eventsMap.addEvent(Cache1.class, CacheEntryEvictedEvent.class, event);
   }

   private void observeCache2CacheEntryEvictedEvent(@Observes @Cache2 CacheEntryEvictedEvent event) {
      eventsMap.addEvent(Cache2.class, CacheEntryEvictedEvent.class, event);
   }

   /**
    * @return Gets events map
    */
   public CacheEventHolder getEventsMap() {
      return eventsMap;
   }

   /**
    * Clears all event from events map.
    */
   public void clear() {
      eventsMap.clear();
   }
}
