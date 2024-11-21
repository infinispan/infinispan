package org.infinispan.client.hotrod.event.impl;

import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;

public class ModifiedEventImpl<K> extends AbstractClientEvent implements ClientCacheEntryModifiedEvent<K> {
   private final K key;
   private final long version;
   private final boolean retried;

   public ModifiedEventImpl(byte[] listenerId, K key, long version, boolean retried) {
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
      return Type.CLIENT_CACHE_ENTRY_MODIFIED;
   }

   @Override
   public String toString() {
      return "ModifiedEventImpl(" + "key=" + key
            + ", dataVersion=" + version + ")";
   }
}
