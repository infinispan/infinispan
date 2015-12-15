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
 * A container of continuous query listeners for a cache.
 * <p>This class is not threadsafe.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class ContinuousQuery<K, V> {

   private final Cache<K, V> cache;

   private final List<EntryListener<K, V, ?>> listeners = new ArrayList<EntryListener<K, V, ?>>();

   public ContinuousQuery(Cache<K, V> cache) {
      this.cache = cache;
   }

   public <C> void addContinuousQueryListener(Query query, ContinuousQueryListener<K, C> listener) {
      EntryListener<K, V, C> entryListener = new EntryListener<K, V, C>(listener);
      cache.addListener(entryListener, makeFilter(query), null);
      listeners.add(entryListener);
   }

   public void removeContinuousQueryListener(ContinuousQueryListener<?, ?> listener) {
      for (Iterator<EntryListener<K, V, ?>> it = listeners.iterator(); it.hasNext(); ) {
         EntryListener<K, V, ?> l = it.next();
         if (l.listener == listener) {
            cache.removeListener(l);
            it.remove();
            break;
         }
      }
   }

   public List<EntryListener<K, V, ?>> getListeners() {
      return listeners;
   }

   public void removeAllListeners() {
      for (EntryListener<K, V, ?> l : listeners) {
         cache.removeListener(l);
      }
   }

   private JPAContinuousQueryCacheEventFilterConverter<K, V, ContinuousQueryResult<V>> makeFilter(Query query) {
      BaseQuery baseQuery = (BaseQuery) query;
      return new JPAContinuousQueryCacheEventFilterConverter<K, V, ContinuousQueryResult<V>>(baseQuery.getJPAQuery(), baseQuery.getNamedParameters(), ReflectionMatcher.class);
   }

   @Listener(clustered = true, includeCurrentState = true, observation = Listener.Observation.POST)
   private static final class EntryListener<K, V, C> {

      private final ContinuousQueryListener<K, C> listener;

      EntryListener(ContinuousQueryListener<K, C> listener) {
         this.listener = listener;
      }

      @CacheEntryRemoved
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryExpired
      public void handleEvent(CacheEntryEvent<K, ContinuousQueryResult<V>> event) {
         ContinuousQueryResult<V> cqr = event.getValue();
         if (cqr.isJoining()) {
            C value = cqr.getValue() != null ? (C) cqr.getValue() : (C) cqr.getProjection();
            listener.resultJoining(event.getKey(), value);
         } else {
            listener.resultLeaving(event.getKey());
         }
      }
   }
}
