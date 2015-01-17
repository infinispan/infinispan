package org.infinispan.jcache.remote;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.AbstractJCacheNotifier;

@ClientListener
public class JCacheListenerAdapter<K, V> extends AbstractJCacheListenerAdapter<K, V> {
   public JCacheListenerAdapter(AbstractJCache<K, V> jcache, AbstractJCacheNotifier<K, V> notifier) {
      super(jcache, notifier);
   }

   @ClientCacheEntryCreated
   public void handleCacheEntryCreatedEvent(ClientCacheEntryCreatedEvent<K> e) {
      K key = e.getKey();
      //TODO: this opens the door to race-conditions
      V value = jcache.get(key);

      notifier.notifyEntryCreated(jcache, key, value);
   }

   @ClientCacheEntryModified
   public void handleCacheEntryModifiedEvent(ClientCacheEntryModifiedEvent<K> e) {
      K key = e.getKey();
      //TODO: this opens the door to race-conditions
      V value = jcache.get(key);

      notifier.notifyEntryUpdated(jcache, key, value);
   }

   @ClientCacheEntryRemoved
   public void handleCacheEntryRemovedEvent(ClientCacheEntryRemovedEvent<K> e) {
      K key = e.getKey();
      //TODO: this opens the door to race-conditions
      V value = jcache.get(key);

      notifier.notifyEntryRemoved(jcache, key, value);
   }
}
