package org.infinispan.query.continuous;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.continuous.impl.ContinuousQueryResult;
import org.infinispan.query.continuous.impl.JPAContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQuery<K, V> {

   private Cache<K, V> cache;

   private List<EntryListener<K, V>> listeners = new ArrayList<EntryListener<K, V>>();

   public ContinuousQuery(Cache<K, V> cache) {
      this.cache = cache;
   }

   public void addContinuousQueryListener(Query query, ContinuousQueryListener<K, V> listener) {
      EntryListener<K, V> entryListener = new EntryListener<K, V>(listener);
      cache.addListener(entryListener, makeFilter(query), null);
      listeners.add(entryListener);
   }

   public void removeContinuousQueryListener(ContinuousQueryListener<K, V> listener) {
      for (Iterator<EntryListener<K, V>> it = listeners.iterator(); it.hasNext(); ) {
         EntryListener<K, V> l = it.next();
         if (l.listener == listener) {
            cache.removeListener(l);
            it.remove();
            break;
         }
      }
   }

   private JPAContinuousQueryCacheEventFilterConverter<K, V, ContinuousQueryResult<V>> makeFilter(Query query) {
      BaseQuery baseQuery = (BaseQuery) query;
      return new JPAContinuousQueryCacheEventFilterConverter<K, V, ContinuousQueryResult<V>>(baseQuery.getJPAQuery(), baseQuery.getNamedParameters(), ReflectionMatcher.class);
   }

   @Listener(clustered = true, includeCurrentState = true, observation = Listener.Observation.POST)
   public static class EntryListener<K, V> {

      private final ContinuousQueryListener<K, V> listener;

      public EntryListener(ContinuousQueryListener<K, V> listener) {
         this.listener = listener;
      }

      @CacheEntryRemoved
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryExpired
      public void handleEvent(CacheEntryEvent<K, ?> event) {
         ContinuousQueryResult<V> cqr = (ContinuousQueryResult<V>) event.getValue();
         if (cqr.isJoining()) {
            listener.resultJoining(event.getKey(), cqr.getValue());
         } else {
            listener.resultLeaving(event.getKey());
         }
      }
   }
}
