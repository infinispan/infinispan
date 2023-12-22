package org.infinispan.embedded;

import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.AdvancedCache;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.sync.SyncStreamingCache;

/**
 * @param <K>
 * @since 15.0
 */
public class EmbeddedSyncStreamingCache<K> implements SyncStreamingCache<K> {
   private final AdvancedCache<K, ?> cache;

   EmbeddedSyncStreamingCache(AdvancedCache<K, ?> cache) {
      this.cache = cache;
   }

   @Override
   public <T extends InputStream & CacheEntryMetadata> T get(K key, CacheOptions options) {
      return null;
   }

   @Override
   public OutputStream put(K key, CacheOptions options) {
      return null;
   }

   @Override
   public OutputStream putIfAbsent(K key, CacheWriteOptions options) {
      return null;
   }
}
