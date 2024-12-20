package org.infinispan.metadata.impl;

import static java.lang.Math.min;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Deprecated(forRemoval=true, since = "10.0")
@ProtoTypeId(ProtoStreamTypeIds.INTERNAL_METADATA_IMPL)
public class InternalMetadataImpl implements InternalMetadata {
   private final Metadata actual;
   private final long created;
   private final long lastUsed;

   @ProtoFactory
   InternalMetadataImpl(long created, long lastUsed, MarshallableObject<Metadata> wrappedMetadata) {
      this(MarshallableObject.unwrap(wrappedMetadata), created, lastUsed);
   }

   public InternalMetadataImpl(InternalCacheEntry ice) {
      this(ice.getMetadata(), ice.getCreated(), ice.getLastUsed());
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
   public boolean isEmpty() {
      return actual.isEmpty();
   }

   @Override
   public Builder builder() {
      return new InternalBuilder(actual, created, lastUsed);
   }

   @Override
   @ProtoField(value = 1, defaultValue = "-1")
   public long created() {
      return created;
   }

   @Override
   @ProtoField(value = 2, defaultValue = "-1")
   public long lastUsed() {
      return lastUsed;
   }

   public Metadata actual() {
      return actual;
   }

   @ProtoField(value = 3)
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(actual);
   }

   static class InternalBuilder implements Builder {
      private Builder actualBuilder;
      private final long created;
      private final long lastUsed;

      InternalBuilder(Metadata actual, long created, long lastUsed) {
         actualBuilder = actual != null ? actual.builder() : new EmbeddedMetadata.Builder();
         this.created = created;
         this.lastUsed = lastUsed;
      }

      @Override
      public Builder lifespan(long time, TimeUnit unit) {
         actualBuilder = actualBuilder.lifespan(time, unit);
         return this;
      }

      @Override
      public Builder lifespan(long time) {
         actualBuilder = actualBuilder.lifespan(time);
         return this;
      }

      @Override
      public Builder maxIdle(long time, TimeUnit unit) {
         actualBuilder = actualBuilder.maxIdle(time, unit);
         return this;
      }

      @Override
      public Builder maxIdle(long time) {
         actualBuilder = actualBuilder.maxIdle(time);
         return this;
      }

      @Override
      public Builder version(EntryVersion version) {
         actualBuilder = actualBuilder.version(version);
         return this;
      }

      @Override
      public Builder merge(Metadata metadata) {
         actualBuilder = actualBuilder.merge(metadata);
         return this;
      }

      @Override
      public Metadata build() {
         return new InternalMetadataImpl(actualBuilder.build(), created, lastUsed);
      }
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
