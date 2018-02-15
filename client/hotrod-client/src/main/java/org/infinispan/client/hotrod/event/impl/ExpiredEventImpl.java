package org.infinispan.client.hotrod.event.impl;

import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;

public class ExpiredEventImpl<K> extends AbstractClientEvent implements ClientCacheEntryExpiredEvent<K> {
   private final K key;

   public ExpiredEventImpl(byte[] listenerId, K key) {
      super(listenerId);
      this.key = key;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public Type getType() {
      return Type.CLIENT_CACHE_ENTRY_EXPIRED;
   }

   @Override
   public String toString() {
      return "ExpiredEventImpl(" + "key=" + key + ")";
   }
}
