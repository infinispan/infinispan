package org.infinispan.jcache;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.jcache.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * JCache notifications dispatcher.
 *
 * TODO: Deal with asynchronous listeners...
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheNotifier<K, V> {

   private static final Log log =
         LogFactory.getLog(JCacheNotifier.class, Log.class);

   private static final boolean isTrace = log.isTraceEnabled();

   // Traversals are a not more common than mutations when it comes to
   // keeping track of registered listeners, so use copy-on-write lists.

   private final List<CacheEntryCreatedListener<K, V>> createdListeners =
         new CopyOnWriteArrayList<CacheEntryCreatedListener<K, V>>();

   private final List<CacheEntryUpdatedListener<K, V>> updatedListeners =
         new CopyOnWriteArrayList<CacheEntryUpdatedListener<K, V>>();

   private final List<CacheEntryRemovedListener<K, V>> removedListeners =
         new CopyOnWriteArrayList<CacheEntryRemovedListener<K, V>>();

   private final List<CacheEntryExpiredListener<K, V>> expiredListeners =
         new CopyOnWriteArrayList<CacheEntryExpiredListener<K, V>>();

   private final ConcurrentMap<CacheEntryListener<? super K, ? super V>, CacheEntryListenerConfiguration<K, V>> listenerCfgs =
         CollectionFactory.makeConcurrentMap();

   public void addListener(CacheEntryListenerConfiguration<K, V> reg) {
      addListener(reg, false);
   }

   @SuppressWarnings("unchecked")
   public void notifyEntryCreated(Cache<K, V> cache, K key, V value) {
      if (!createdListeners.isEmpty()) {
         List<CacheEntryEvent<? extends K, ? extends V>> events =
               createEvent(cache, key, value, EventType.CREATED);
         for (CacheEntryCreatedListener<K, V> listener : createdListeners)
            listener.onCreated(getEntryIterable(events, listenerCfgs.get(listener)));
      }
   }

   @SuppressWarnings("unchecked")
   public void notifyEntryUpdated(Cache<K, V> cache, K key, V value) {
      if (!updatedListeners.isEmpty()) {
         List<CacheEntryEvent<? extends K, ? extends V>> events =
               createEvent(cache, key, value, EventType.UPDATED);
         for (CacheEntryUpdatedListener<K, V> listener : updatedListeners)
            listener.onUpdated(getEntryIterable(events, listenerCfgs.get(listener)));
      }
   }

   @SuppressWarnings("unchecked")
   public void notifyEntryRemoved(Cache<K, V> cache, K key, V value) {
      if (!removedListeners.isEmpty()) {
         List<CacheEntryEvent<? extends K, ? extends V>> events =
               createEvent(cache, key, value, EventType.REMOVED);
         for (CacheEntryRemovedListener<K, V> listener : removedListeners) {
            listener.onRemoved(getEntryIterable(events, listenerCfgs.get(listener)));
         }
      }
   }

   @SuppressWarnings("unchecked")
   public void notifyEntryExpired(Cache<K, V> cache, K key, V value) {
      if (!expiredListeners.isEmpty()) {
         List<CacheEntryEvent<? extends K, ? extends V>> events =
               createEvent(cache, key, value, EventType.EXPIRED);
         for (CacheEntryExpiredListener<K, V> listener : expiredListeners) {
            listener.onExpired(getEntryIterable(events, listenerCfgs.get(listener)));
         }
      }
   }

   private Iterable<CacheEntryEvent<? extends K, ? extends V>> getEntryIterable(
         List<CacheEntryEvent<? extends K, ? extends V>> events,
         CacheEntryListenerConfiguration<K, V> listenerCfg) {
      Factory<CacheEntryEventFilter<? super K,? super V>> factory = listenerCfg.getCacheEntryEventFilterFactory();
      if (factory != null) {
         CacheEntryEventFilter<? super K, ? super V> filter = factory.create();
         return filter == null  ? events
               : new JCacheEventFilteringIterable<K, V>(events, filter);
      }

      return events;
   }

   @SuppressWarnings("unchecked")
   private boolean addListener(CacheEntryListenerConfiguration<K, V> listenerCfg, boolean addIfAbsent) {
      boolean added = false;
      CacheEntryListener<? super K, ? super V> listener =
            listenerCfg.getCacheEntryListenerFactory().create();
      if (listener instanceof CacheEntryCreatedListener)
         added = !containsListener(addIfAbsent, listener, createdListeners)
               && createdListeners.add((CacheEntryCreatedListener<K, V>) listener);

      if (listener instanceof CacheEntryUpdatedListener)
         added = !containsListener(addIfAbsent, listener, updatedListeners)
               && updatedListeners.add((CacheEntryUpdatedListener<K, V>) listener);

      if (listener instanceof CacheEntryRemovedListener)
         added = !containsListener(addIfAbsent, listener, removedListeners)
               && removedListeners.add((CacheEntryRemovedListener<K, V>) listener);

      if (listener instanceof CacheEntryExpiredListener)
         added = !containsListener(addIfAbsent, listener, expiredListeners)
               && expiredListeners.add((CacheEntryExpiredListener<K, V>) listener);

      if (added)
         listenerCfgs.put(listener, listenerCfg);

      return added;
   }

   private boolean containsListener(boolean addIfAbsent,
         CacheEntryListener<? super K, ? super V> listenerToAdd,
         List<? extends CacheEntryListener<? super K, ? super V>> listeners) {
      // If add only if no listener present, check the listeners collection
      if (addIfAbsent) {
         for (CacheEntryListener<? super K, ? super V> listener : listeners) {
            if (listener.equals(listenerToAdd))
               return true;
         }
      }

      return false;
   }

   private List<CacheEntryEvent<? extends K, ? extends V>> createEvent(
         Cache<K, V> cache, K key, V value, EventType eventType) {
      List<CacheEntryEvent<? extends K, ? extends V>> events =
            Collections.<CacheEntryEvent<? extends K, ? extends V>>singletonList(
                  new RICacheEntryEvent<K, V>(cache, key, value, eventType));
      if (isTrace) log.tracef("Received event: %s", events);
      return events;
   }

}
