package org.infinispan.client.hotrod;

import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.sync.SyncStreamingCache;

@Experimental
final class HotRodSyncStreamingCache<K> implements SyncStreamingCache<K> {

   HotRodSyncStreamingCache() {}

   @Override
   public <T extends InputStream & CacheEntryMetadata> T get(K key, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public OutputStream put(K key, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public OutputStream putIfAbsent(K key, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }
}
