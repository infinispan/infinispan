package org.infinispan.embedded.impl;

import java.time.Instant;
import java.util.Optional;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheEntryVersion;

public class EmbeddedCacheEntry<K, V> implements CacheEntry<K, V> {
   private final org.infinispan.container.entries.CacheEntry<K, V> entry;

   public EmbeddedCacheEntry(org.infinispan.container.entries.CacheEntry<K, V> entry) {
      this.entry = entry;
   }

   @Override
   public K key() {
      return entry.getKey();
   }

   @Override
   public V value() {
      return entry.getValue();
   }

   @Override
   public CacheEntryMetadata metadata() {
      return new CacheEntryMetadata() {
         @Override
         public Optional<Instant> creationTime() {
            return Optional.empty();
         }

         @Override
         public Optional<Instant> lastAccessTime() {
            return Optional.empty();
         }

         @Override
         public CacheEntryExpiration expiration() {
            return null;
         }

         @Override
         public CacheEntryVersion version() {
            return null;
         }
      };
   }
}
