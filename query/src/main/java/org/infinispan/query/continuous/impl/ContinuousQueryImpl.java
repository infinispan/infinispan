package org.infinispan.query.continuous.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;

/**
 * A container of continuous query listeners for a cache.
 * <p>This class is not threadsafe.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public final class ContinuousQueryImpl<K, V> implements ContinuousQuery<K, V> {

   private final Cache<K, V> cache;

   private final List<EntryListener<K, V, ?>> listeners = new ArrayList<>();

   public ContinuousQueryImpl(Cache<K, V> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter cannot be null");
      }
      this.cache = cache;
   }

   @Override
   public <C> void addContinuousQueryListener(String queryString, ContinuousQueryListener<K, C> listener) {
      addContinuousQueryListener(queryString, null, listener);
   }

   @Override
   public <C> void addContinuousQueryListener(String queryString, Map<String, Object> namedParameters, ContinuousQueryListener<K, C> listener) {
      EntryListener<K, V, C> entryListener = new EntryListener<>(listener);
      IckleContinuousQueryCacheEventFilterConverter<K, V, ContinuousQueryResult<V>> filterConverter
            = new IckleContinuousQueryCacheEventFilterConverter<>(queryString, namedParameters, ReflectionMatcher.class);
      cache.addListener(entryListener, filterConverter, null);
      listeners.add(entryListener);
   }

   @Override
   public <C> void addContinuousQueryListener(Query query, ContinuousQueryListener<K, C> listener) {
      addContinuousQueryListener(query.getQueryString(), query.getParameters(), listener);
   }

   @Override
   public void removeContinuousQueryListener(ContinuousQueryListener<K, ?> listener) {
      for (Iterator<EntryListener<K, V, ?>> it = listeners.iterator(); it.hasNext(); ) {
         EntryListener<K, V, ?> l = it.next();
         if (l.listener == listener) {
            cache.removeListener(l);
            it.remove();
            break;
         }
      }
   }

   @Override
   public List<ContinuousQueryListener<K, ?>> getListeners() {
      List<ContinuousQueryListener<K, ?>> queryListeners = new ArrayList<>(listeners.size());
      for (EntryListener<K, V, ?> l : listeners) {
         queryListeners.add(l.listener);
      }
      return queryListeners;
   }

   @Override
   public void removeAllListeners() {
      for (EntryListener<K, V, ?> l : listeners) {
         cache.removeListener(l);
      }
      listeners.clear();
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
         switch (cqr.getResultType()) {
            case JOINING: {
               C value = cqr.getValue() != null ? (C) cqr.getValue() : (C) cqr.getProjection();
               listener.resultJoining(event.getKey(), value);
               break;
            }
            case UPDATED: {
               C value = cqr.getValue() != null ? (C) cqr.getValue() : (C) cqr.getProjection();
               listener.resultUpdated(event.getKey(), value);
               break;
            }
            case LEAVING: {
               listener.resultLeaving(event.getKey());
               break;
            }
            default:
               throw new IllegalStateException("Unexpected result type : " + cqr.getResultType());
         }
      }
   }
}
