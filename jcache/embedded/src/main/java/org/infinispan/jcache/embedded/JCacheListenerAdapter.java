package org.infinispan.jcache.embedded;

import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.AbstractJCacheNotifier;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

/**
 * Adapts Infinispan notification mechanism to JSR 107 requirements.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
@Listener
public class JCacheListenerAdapter<K, V> extends AbstractJCacheListenerAdapter<K, V> {
   public JCacheListenerAdapter(AbstractJCache<K, V> jcache, AbstractJCacheNotifier<K, V> notifier) {
      super(jcache, notifier);
   }

   @CacheEntryCreated
   public void handleCacheEntryCreatedEvent(CacheEntryCreatedEvent<K, V> e) {
      // JCache listeners notified only once, so do it after the event
      if (!e.isPre())
         notifier.notifyEntryCreated(jcache, e.getKey(), e.getValue());
   }

   @CacheEntryModified
   public void handleCacheEntryModifiedEvent(CacheEntryModifiedEvent<K, V> e) {
      // JCache listeners notified only once, so do it after the event
      if (!e.isPre() && !e.isCreated())
         notifier.notifyEntryUpdated(jcache, e.getKey(), e.getValue());
   }

   @CacheEntryRemoved
   public void handleCacheEntryRemovedEvent(CacheEntryRemovedEvent<K, V> e) {
      // JCache listeners notified only once, so do it after the event
      if (!e.isPre())
         notifier.notifyEntryRemoved(jcache, e.getKey(), e.getOldValue());
   }
}
