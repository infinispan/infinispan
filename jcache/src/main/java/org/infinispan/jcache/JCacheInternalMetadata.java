package org.infinispan.jcache;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.InternalMetadata;

/**
 * Metadata for entries stored via JCache API
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class JCacheInternalMetadata implements InternalMetadata {

   private final long created; // absolute time of creation
   private final long expiry; // absolute time when entry should expire

   public JCacheInternalMetadata(long created, long expiry) {
      this.created = created;
      this.expiry = expiry;
   }

   @Override
   public long created() {
      return created;
   }

   @Override
   public long lastUsed() {
      return 0;
   }

   @Override
   public boolean isExpired(long now) {
      return expiry > -1 && expiry <= now;
   }

   @Override
   public long expiryTime() {
      return expiry;
   }

   @Override
   public long lifespan() {
      return expiry - created;
   }

   @Override
   public long maxIdle() {
      return -1;
   }

   @Override
   public EntryVersion version() {
      return null;
   }

   @Override
   public Builder builder() {
      return null;
   }

}
