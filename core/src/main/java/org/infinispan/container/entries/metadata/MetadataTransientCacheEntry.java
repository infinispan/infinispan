package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A cache entry that is transient, i.e., it can be considered expired after a period of not being used, and {@link
 * MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_TRANSIENT_CACHE_ENTRY)
public class MetadataTransientCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected Metadata metadata;
   protected long lastUsed;

   public MetadataTransientCacheEntry(Object key, Object value, Metadata metadata, long lastUsed) {
      this(key, value, null, metadata, lastUsed);
   }

   protected MetadataTransientCacheEntry(Object key, Object value, PrivateMetadata internalMetadata,
         Metadata metadata, long lastUsed) {
      super(key, value, internalMetadata);
      this.metadata = metadata;
      this.lastUsed = lastUsed;
   }

   @ProtoFactory
   protected MetadataTransientCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                                         PrivateMetadata internalMetadata, MarshallableObject<Metadata> wrappedMetadata,
                                        long lastUsed) {
      super(wrappedKey, wrappedValue, internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.lastUsed = lastUsed;
   }

   @ProtoField(number = 4, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(number = 5, defaultValue = "-1")
   public final long getLastUsed() {
      return lastUsed;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      lastUsed = currentTimeMillis;
   }


   @Override
   public void reincarnate(long now) {
      //no-op
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public boolean canExpireMaxIdle() {
      return true;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(metadata.maxIdle(), lastUsed, now);
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      long maxIdle = metadata.maxIdle();
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   public final long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new MetadataTransientCacheValue(value, internalMetadata, metadata, lastUsed);
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", lastUsed=").append(lastUsed);
   }
}
