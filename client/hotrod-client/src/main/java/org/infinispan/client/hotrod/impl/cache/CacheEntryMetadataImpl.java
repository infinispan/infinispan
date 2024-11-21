package org.infinispan.client.hotrod.impl.cache;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.client.hotrod.MetadataValue;

public class CacheEntryMetadataImpl<V> implements CacheEntryMetadata {
   private final MetadataValue<V> metadataValue;
   private final CacheEntryExpiration expiration;
   private final CacheEntryVersion version;

   public CacheEntryMetadataImpl(MetadataValue<V> metadataValue) {
      this.metadataValue = metadataValue;
      this.expiration = getCacheExpiration(metadataValue.getLifespan(), metadataValue.getMaxIdle());
      this.version = new CacheEntryVersionImpl(metadataValue.getVersion());
   }

   @Override
   public Optional<Instant> creationTime() {
      return metadataValue.getCreated() < 0
            ? Optional.empty()
            : Optional.of(Instant.ofEpochMilli(metadataValue.getCreated()));
   }

   @Override
   public Optional<Instant> lastAccessTime() {
      return metadataValue.getLastUsed() < 0
            ? Optional.empty()
            : Optional.of(Instant.ofEpochMilli(metadataValue.getLastUsed()));
   }

   @Override
   public CacheEntryExpiration expiration() {
      return expiration;
   }

   @Override
   public CacheEntryVersion version() {
      return version;
   }

   public MetadataValue<V> getMetadataValue() {
      return metadataValue;
   }

   private static CacheEntryExpiration getCacheExpiration(long lifespan, long maxIdle) {
      if (lifespan < 0) {
         if (maxIdle < 0) return CacheEntryExpiration.IMMORTAL;
         return CacheEntryExpiration.withMaxIdle(Duration.ofSeconds(maxIdle));
      }

      if (maxIdle < 0) return CacheEntryExpiration.withLifespan(Duration.ofSeconds(lifespan));
      return CacheEntryExpiration.withLifespanAndMaxIdle(Duration.ofSeconds(lifespan), Duration.ofSeconds(maxIdle));
   }
}
