package org.infinispan.client.hotrod.event.impl;

import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;

public class RemovedEventImpl<K> extends AbstractClientEvent implements ClientCacheEntryRemovedEvent<K> {
   private final K key;
   private final boolean retried;

   public RemovedEventImpl(byte[] listenerId, K key, boolean retried) {
      super(listenerId);
      this.key = key;
      this.retried = retried;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public boolean isCommandRetried() {
      return retried;
   }

   @Override
   public Type getType() {
      return Type.CLIENT_CACHE_ENTRY_REMOVED;
   }

   @Override
   public String toString() {
      return "RemovedEventImpl(" + "key=" + key + ")";
   }
}
