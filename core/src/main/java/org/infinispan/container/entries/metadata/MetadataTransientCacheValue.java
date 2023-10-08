package org.infinispan.container.entries.metadata;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A transient cache value, to correspond with {@link TransientCacheEntry} which is {@link MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ProtoTypeId(ProtoStreamTypeIds.METADATA_TRANSIENT_CACHE_VALUE)
public class MetadataTransientCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;
   long lastUsed;

   public MetadataTransientCacheValue(Object value, Metadata metadata, long lastUsed) {
      this(value, null, metadata, lastUsed);
   }

   protected MetadataTransientCacheValue(Object value, PrivateMetadata internalMetadata, Metadata metadata,
         long lastUsed) {
      super(value, internalMetadata);
      this.metadata = metadata;
      this.lastUsed = lastUsed;
   }

   @ProtoFactory
   protected MetadataTransientCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata,
                                         MarshallableObject<Metadata> wrappedMetadata, long lastUsed) {
      super(wrappedValue, internalMetadata);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.lastUsed = lastUsed;
   }

   @ProtoField(number = 4, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(5)
   public final long getLastUsed() {
      return lastUsed;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MetadataTransientCacheEntry(key, value, internalMetadata, metadata, lastUsed);
   }

   @Override
   public long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(metadata.maxIdle(), lastUsed, now);
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
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
   public long getExpiryTime() {
      long maxIdle = metadata.maxIdle();
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", lastUsed=").append(lastUsed);
   }
}
