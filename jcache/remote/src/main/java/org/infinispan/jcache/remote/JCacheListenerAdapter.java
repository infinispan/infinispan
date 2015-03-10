package org.infinispan.jcache.remote;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.commons.util.KeyValueWithPrevious;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.AbstractJCacheNotifier;

@ClientListener(converterFactoryName = "key-value-with-previous-converter-factory")
public class JCacheListenerAdapter<K, V> extends AbstractJCacheListenerAdapter<K, V> {
   public JCacheListenerAdapter(AbstractJCache<K, V> jcache, AbstractJCacheNotifier<K, V> notifier) {
      super(jcache, notifier);
   }

   @ClientCacheEntryCreated
   @ClientCacheEntryModified
   @ClientCacheEntryRemoved
   public void handleCacheEntryEvent(ClientCacheEntryCustomEvent<KeyValueWithPrevious<K, V>> e) {
      KeyValueWithPrevious<K, V> event = e.getEventData();
      switch (e.getType()) {
      case CLIENT_CACHE_ENTRY_CREATED: {
         notifier.notifyEntryCreated(jcache, event.getKey(), event.getValue());
         break;
      }
      case CLIENT_CACHE_ENTRY_REMOVED: {
         notifier.notifyEntryRemoved(jcache, event.getKey(), event.getPrev());
         break;
      }
      case CLIENT_CACHE_ENTRY_MODIFIED: {
         notifier.notifyEntryUpdated(jcache, event.getKey(), event.getValue());
         break;
      }
      case CLIENT_CACHE_FAILOVER:
         // No JSR107 correspondent.
         break;
      default:
         break;
      }
   }
}
