package org.infinispan.container.entries;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A cache entry that is immortal/cannot expire
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IMMORTAL_CACHE_ENTRY)
public class ImmortalCacheEntry extends AbstractInternalCacheEntry {

   @ProtoFactory
   ImmortalCacheEntry(MarshallableObject<?> wrappedKey, MarshallableObject<?> wrappedValue,
                      PrivateMetadata internalMetadata) {
      super(wrappedKey, wrappedValue, internalMetadata);
   }

   public ImmortalCacheEntry(Object key, Object value) {
      this(key, value, null);
   }

   public ImmortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata) {
      super(key, value, internalMetadata);
   }

   @Override
   public final boolean isExpired(long now) {
      return false;
   }

   @Override
   public final boolean canExpire() {
      return false;
   }

   @Override
   public final long getCreated() {
      return -1;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return -1;
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      return -1;
   }

   @Override
   public void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      // no-op
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new ImmortalCacheValue(value, internalMetadata);
   }

   @Override
   public Metadata getMetadata() {
      return EmbeddedMetadata.EMPTY;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on immortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public ImmortalCacheEntry clone() {
      return (ImmortalCacheEntry) super.clone();
   }
}
