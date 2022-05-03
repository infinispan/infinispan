package org.infinispan.hotrod;

import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.sync.SyncStreamingCache;
import org.infinispan.hotrod.impl.cache.RemoteCache;

/**
 * @since 14.0
 **/
public class HotRodSyncStreamingCache<K> implements SyncStreamingCache<K> {
   private final HotRod hotrod;
   private final RemoteCache<K, ?> remoteCache;

   HotRodSyncStreamingCache(HotRod hotrod, RemoteCache<K, ?> remoteCache) {
      this.hotrod = hotrod;
      this.remoteCache = remoteCache;
   }

   @Override
   public <T extends InputStream & CacheEntryMetadata> T get(K key) {
      throw new UnsupportedOperationException();
   }

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
