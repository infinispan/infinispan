package org.infinispan.metadata.impl;

import static java.lang.Math.min;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Deprecated
public class InternalMetadataImpl implements InternalMetadata {
   private final Metadata actual;
   private final long created;
   private final long lastUsed;

   public InternalMetadataImpl() {
      //required by the AZUL VM in order to run the tests
      this(null, -1, -1);
   }

   public InternalMetadataImpl(InternalCacheEntry ice) {
      this(ice.getMetadata(), ice.getCreated(), ice.getLastUsed());
   }

   public InternalMetadataImpl(InternalCacheValue icv) {
      this(icv.getMetadata(), icv.getCreated(), icv.getLastUsed());
   }

   public InternalMetadataImpl(Metadata actual, long created, long lastUsed) {
      this.actual = extractMetadata(actual);
      this.created = created;
      this.lastUsed = lastUsed;
   }

   @Override
   public long lifespan() {
      return actual.lifespan();
   }

   @Override
   public long maxIdle() {
      return actual.maxIdle();
   }

   @Override
   public EntryVersion version() {
      return actual.version();
   }

   @Override
   public Builder builder() {
      return actual.builder();
   }

   @Override
   public long created() {
      return created;
   }

   @Override
   public long lastUsed() {
      return lastUsed;
   }

   public Metadata actual() {
      return actual;
   }

   @Override
   public long expiryTime() {
      long lifespan = actual.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = actual.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   public boolean isExpired(long now) {
      long expiry = expiryTime();
      return expiry > 0 && expiry <= now;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof InternalMetadataImpl)) return false;

      InternalMetadataImpl that = (InternalMetadataImpl) o;

      if (created != that.created) return false;
      if (lastUsed != that.lastUsed) return false;
      if (actual != null ? !actual.equals(that.actual) : that.actual != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = actual != null ? actual.hashCode() : 0;
      result = 31 * result + (int) (created ^ (created >>> 32));
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "InternalMetadataImpl{" +
            "actual=" + actual +
            ", created=" + created +
            ", lastUsed=" + lastUsed +
            '}';
   }

   public static Metadata extractMetadata(Metadata metadata) {
      Metadata toCheck = metadata;
      while (toCheck != null) {
         if (toCheck instanceof InternalMetadataImpl) {
            toCheck = ((InternalMetadataImpl) toCheck).actual();
         } else {
            break;
         }
      }
      return toCheck;
   }
}
