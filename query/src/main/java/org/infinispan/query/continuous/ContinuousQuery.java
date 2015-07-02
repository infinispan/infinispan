package org.infinispan.query.continuous;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
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

   public void addContinuousQueryListener(Query query, ContinuousQueryResultListener<K, V> listener) {
      EntryListener<K, V> entryListener = new EntryListener<K, V>(listener);
      cache.addListener(entryListener, makeFilter(query), null);
      listeners.add(entryListener);
   }

   public void removeContinuousQueryListener(ContinuousQueryResultListener<K, V> listener) {
      for (Iterator<EntryListener<K, V>> it = listeners.iterator(); it.hasNext(); ) {
         EntryListener<K, V> l = it.next();
         if (l.listener == listener) {
            cache.removeListener(l);
            it.remove();
            break;
         }
      }
   }

   private JPAContinuousQueryCacheEventFilterConverter<K, V> makeFilter(Query query) {
      return new JPAContinuousQueryCacheEventFilterConverter<K, V>(((BaseQuery) query).getJPAQuery(), ReflectionMatcher.class);
   }

   @Listener(observation = Listener.Observation.POST)
   public static class EntryListener<K, V> {

      private final ContinuousQueryResultListener<K, V> listener;

      public EntryListener(ContinuousQueryResultListener<K, V> listener) {
         this.listener = listener;
      }

      @CacheEntryRemoved
      @CacheEntryCreated
      @CacheEntryModified
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
