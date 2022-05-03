package org.infinispan.hotrod.impl.cache;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheEntryVersion;

/**
 * @since 14.0
 **/
public class CacheEntryMetadataImpl implements CacheEntryMetadata {
   private final long creation;
   private final long lastAccess;
   private final CacheEntryVersion version;
   private final CacheEntryExpiration expiration;

   public CacheEntryMetadataImpl() {
      this(-1, -1, null, null);
   }

   public CacheEntryMetadataImpl(long creation, long lastAccess, CacheEntryExpiration expiration, CacheEntryVersion version) {
      this.creation = creation;
      this.lastAccess = lastAccess;
      this.expiration = expiration;
      this.version = version;
   }

   @Override
   public Optional<Instant> creationTime() {
      return creation < 0 ? Optional.empty() : Optional.of(Instant.ofEpochMilli(creation));
   }

   @Override
   public Optional<Instant> lastAccessTime() {
      return lastAccess < 0 ? Optional.empty() : Optional.of(Instant.ofEpochMilli(lastAccess));
   }

   @Override
   public CacheEntryExpiration expiration() {
      return expiration;
   }

   @Override
   public CacheEntryVersion version() {
      return version;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheEntryMetadataImpl that = (CacheEntryMetadataImpl) o;
      return creation == that.creation && lastAccess == that.lastAccess && Objects.equals(version, that.version) && Objects.equals(expiration, that.expiration);
   }

   @Override
   public int hashCode() {
      return Objects.hash(creation, lastAccess, version, expiration);
   }

   @Override
   public String toString() {
      return "CacheEntryMetadataImpl{" +
            "creation=" + creation +
            ", lastAccess=" + lastAccess +
            ", version=" + version +
            ", expiration=" + expiration +
            '}';
   }
}
