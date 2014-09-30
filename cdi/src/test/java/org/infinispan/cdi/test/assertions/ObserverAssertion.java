package org.infinispan.cdi.test.assertions;

import org.infinispan.cdi.test.event.CacheObserver;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Observer custom assertion.
 *
 * @author Sebastian Laskawiec
 */
public class ObserverAssertion {

   private CacheObserver observer;
   private Class<?> cacheAnnotation;

   private ObserverAssertion(CacheObserver observer, Class<?> cacheAnnotation) {
      this.cacheAnnotation = cacheAnnotation;
      this.observer = observer;
   }

   public static ObserverAssertion assertThat(CacheObserver observer, Class<?> cacheAnnotation) {
      return new ObserverAssertion(observer, cacheAnnotation);
   }

   private <T> List<T> getNonEmptyListOfEvents(Class<T> eventClass) {
      List<T> events = observer.getEventsMap().getEvents(cacheAnnotation, eventClass);
      assertTrue(events.size() > 0);
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

   public ObserverAssertion hasEntryEvictedEvent(String key) {
      assertEquals(getNonEmptyListOfEvents(CacheEntryEvictedEvent.class).get(0).getKey(), key);
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
