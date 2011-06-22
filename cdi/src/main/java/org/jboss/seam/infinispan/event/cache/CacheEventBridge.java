package org.jboss.seam.infinispan.event.cache;

import org.infinispan.notifications.Listenable;
import org.infinispan.notifications.cachelistener.event.Event;
import org.jboss.seam.infinispan.event.AbstractEventBridge;

import java.lang.annotation.Annotation;
import java.util.Set;

public class CacheEventBridge extends AbstractEventBridge<Event<?, ?>> {

   public void registerObservers(Set<Annotation> qualifierSet,
         Listenable listenable) {
      Annotation[] qualifiers = qualifierSet
            .toArray(new Annotation[qualifierSet.size()]);
      if (hasObservers(CacheEntryActivatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryActivatedAdapter(getBaseEvent()
               .select(CacheEntryActivatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryCreatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryCreatedAdapter(getBaseEvent()
               .select(CacheEntryCreatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryEvictedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryEvictedAdapter(getBaseEvent()
               .select(CacheEntryEvictedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryInvalidatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryInvalidatedAdapter(getBaseEvent()
               .select(CacheEntryInvalidatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryLoadedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryLoadedAdapter(getBaseEvent()
               .select(CacheEntryLoadedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryModifiedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryModifiedAdapter(getBaseEvent()
               .select(CacheEntryModifiedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryPassivatedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryPassivatedAdapter(getBaseEvent()
               .select(CacheEntryPassivatedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryRemovedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryRemovedAdapter(getBaseEvent()
               .select(CacheEntryRemovedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(CacheEntryVisitedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new CacheEntryVisitedAdapter(getBaseEvent()
               .select(CacheEntryVisitedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(TransactionCompletedAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new TransactionCompletedAdapter(getBaseEvent()
               .select(TransactionCompletedAdapter.WILDCARD_TYPE, qualifiers)));
      }
      if (hasObservers(TransactionRegisteredAdapter.EMPTY, qualifiers)) {
         listenable.addListener(new TransactionRegisteredAdapter(getBaseEvent()
               .select(TransactionRegisteredAdapter.WILDCARD_TYPE, qualifiers)));
      }
   }

}
