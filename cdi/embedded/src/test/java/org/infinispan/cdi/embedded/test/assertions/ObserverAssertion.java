package org.infinispan.cdi.embedded.test.assertions;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.infinispan.cdi.embedded.test.event.CacheObserver;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * Observer custom assertion.
 *
 * @author Sebastian Laskawiec
 */
public class ObserverAssertion {

   private final CacheObserver observer;
   private final Class<?> cacheAnnotation;

   private ObserverAssertion(CacheObserver observer, Class<?> cacheAnnotation) {
      this.cacheAnnotation = cacheAnnotation;
      this.observer = observer;
   }

   public static ObserverAssertion assertThat(CacheObserver observer, Class<?> cacheAnnotation) {
      return new ObserverAssertion(observer, cacheAnnotation);
   }

   private <T> List<T> getNonEmptyListOfEvents(Class<T> eventClass) {
      List<T> events = observer.getEventsMap().getEvents(cacheAnnotation, eventClass);
      assertTrue(!events.isEmpty());
      return events;
   }

   public ObserverAssertion hasProperName(String cacheName) {
      assertEquals(getNonEmptyListOfEvents(CacheStartedEvent.class).get(0).getCacheName(), cacheName);
      return this;
   }

   public ObserverAssertion hasStartedEvent() {
      getNonEmptyListOfEvents(CacheStartedEvent.class);
      return this;
   }

   public ObserverAssertion hasStoppedEvent() {
      getNonEmptyListOfEvents(CacheStoppedEvent.class);
      return this;
   }

   public ObserverAssertion hasEntryCreatedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryCreatedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryRemovedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryRemovedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryActivatedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryActivatedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntriesEvictedEvent(String key) {
      assertTrue(getNonEmptyListOfEvents(CacheEntriesEvictedEvent.class).get(0).getEntries().containsKey(key));
      return this;
   }

   public ObserverAssertion hasEntryExpiredEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryExpiredEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryModifiedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryModifiedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryInvalidatedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryInvalidatedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryLoadedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryLoadedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryPassivatedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryPassivatedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasEntryVisitedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryVisitedEvent.class).get(0).getKey(), key);
      return this;
   }

   public ObserverAssertion hasDataRehashEvent(ConsistentHash newHash) {
      assertEquals(getNonEmptyListOfEvents(DataRehashedEvent.class).get(0).getConsistentHashAtEnd(), newHash);
      return this;
   }

   public ObserverAssertion hasTransactionCompletedEvent(boolean isSuccesful) {
      assertEquals(getNonEmptyListOfEvents(TransactionCompletedEvent.class).get(0).isTransactionSuccessful(), isSuccesful);
      return this;
   }

   public ObserverAssertion hasTransactionRegisteredEvent(boolean isOriginLocal) {
      assertEquals(getNonEmptyListOfEvents(TransactionRegisteredEvent.class).get(0).isOriginLocal(), isOriginLocal);
      return this;
   }

   public ObserverAssertion hasViewChangedEvent(Address myAddress) {
      assertEquals(getNonEmptyListOfEvents(ViewChangedEvent.class).get(0).getLocalAddress(), myAddress);
      return this;
   }

   public ObserverAssertion hasTopologyChangedEvent(int topologyId) {
      assertEquals(getNonEmptyListOfEvents(TopologyChangedEvent.class).get(0).getNewTopologyId(), topologyId);
      return this;
   }
}
