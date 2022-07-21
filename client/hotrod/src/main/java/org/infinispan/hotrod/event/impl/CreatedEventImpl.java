package org.infinispan.hotrod.event.impl;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CommonCacheEventTypes;
import org.infinispan.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;

public class CreatedEventImpl<K, V> extends AbstractClientEvent implements ClientCacheEntryCreatedEvent<K>, CacheEntryEvent<K, V>, CacheEntry<K, V> {
   final K key;
   final long version;
   final boolean retried;

   public CreatedEventImpl(byte[] listenerId, K key, long version, boolean retried) {
      super(listenerId);
      this.key = key;
      this.version = version;
      this.retried = retried;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public long getVersion() {
      return version;
   }

   @Override
   public boolean isCommandRetried() {
      return retried;
   }

   @Override
   public Type getType() {
      return Type.CLIENT_CACHE_ENTRY_CREATED;
   }

   @Override
   public String toString() {
      return "CreatedEventImpl(" + "key=" + key
            + ", dataVersion=" + version + ")";
   }

   @Override
   public CacheEntry<K, V> newEntry() {
      return this;
   }

   @Override
   public CacheEntry<K, V> previousEntry() {
      return null;
   }

   @Override
   public CacheEntryEventType type() {
      return CommonCacheEventTypes.CREATED;
   }

   @Override
   public K key() {
      return key;
   }

   @Override
   public V value() {
      return null;
   }

   @Override
   public CacheEntryMetadata metadata() {
      return new CacheEntryMetadataImpl(-1, -1, null, new CacheEntryVersionImpl(version));
   }
}
