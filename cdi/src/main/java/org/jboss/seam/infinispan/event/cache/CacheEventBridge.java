package org.jboss.seam.infinispan.event.cache;

import org.infinispan.notifications.Listenable;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.jboss.seam.infinispan.event.AbstractEventBridge;

import java.lang.annotation.Annotation;
import java.util.Set;

public class CacheEventBridge extends AbstractEventBridge<Event> {

   public void registerObservers(Set<Annotation> qualifierSet,
         Listenable listenable) {
      Annotation[] qualifiers = qualifierSet
            .toArray(new Annotation[qualifierSet.size()]);
      if (hasObservers(CacheEntryActivatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryActivatedAdapter(getBaseEvent()
               .select(CacheEntryActivatedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryCreatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryCreatedAdapter(getBaseEvent()
               .select(CacheEntryCreatedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryEvictedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryEvictedAdapter(getBaseEvent()
               .select(CacheEntryEvictedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryInvalidatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryInvalidatedAdapter(getBaseEvent()
               .select(CacheEntryInvalidatedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryLoadedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryLoadedAdapter(getBaseEvent()
               .select(CacheEntryLoadedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryModifiedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryModifiedAdapter(getBaseEvent()
               .select(CacheEntryModifiedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryPassivatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryPassivatedAdapter(getBaseEvent()
               .select(CacheEntryPassivatedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryRemovedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryRemovedAdapter(getBaseEvent()
               .select(CacheEntryRemovedEvent.class, qualifiers)));
      }
      if (hasObservers(CacheEntryVisitedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryVisitedAdapter(getBaseEvent()
               .select(CacheEntryVisitedEvent.class, qualifiers)));
      }
      if (hasObservers(TransactionCompletedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new TransactionCompletedAdapter(getBaseEvent()
               .select(TransactionCompletedEvent.class, qualifiers)));
      }
      if (hasObservers(TransactionRegisteredAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new TransactionRegisteredAdapter(getBaseEvent()
               .select(TransactionRegisteredEvent.class, qualifiers)));
      }
   }

}
