package org.infinispan.client.hotrod.event.impl;

import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;

public class CreatedEventImpl<K> extends AbstractClientEvent implements ClientCacheEntryCreatedEvent<K> {
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
}
