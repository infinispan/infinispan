package org.infinispan.container.entries;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An immortal cache value, to correspond with {@link org.infinispan.container.entries.ImmortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.IMMORTAL_CACHE_VALUE)
public class ImmortalCacheValue implements InternalCacheValue, Cloneable {

   public Object value;
   protected PrivateMetadata internalMetadata;

   public ImmortalCacheValue(Object value) {
      this(value, null);
   }

   public ImmortalCacheValue(Object value, PrivateMetadata internalMetadata) {
      this(MarshallableObject.create(value), internalMetadata);
   }

   @ProtoFactory
   public ImmortalCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata) {
      this.value = MarshallableObject.unwrap(wrappedValue);
      this.internalMetadata = internalMetadata;
   }

   @ProtoField(number = 1, name ="value")
   public MarshallableObject<?> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @Override
   @ProtoField(number = 2)
   public final PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   @Override
   public final void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
   }

   @Override
   public InternalCacheEntry<?,?> toInternalCacheEntry(Object key) {
      return new ImmortalCacheEntry(key, value, internalMetadata);
   }

   public final Object setValue(Object value) {
      Object old = this.value;
      this.value = value;
      return old;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(getLifespan()).maxIdle(getMaxIdle()).build();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ImmortalCacheValue)) return false;

      ImmortalCacheValue that = (ImmortalCacheValue) o;

      return Objects.equals(value, that.value) &&
             Objects.equals(internalMetadata, that.internalMetadata);
   }

   @Override
   public int hashCode() {
      int result = Objects.hashCode(value);
      result = 31 * result + Objects.hashCode(internalMetadata);
      return result;
   }

   @Override
   public final String toString() {
      StringBuilder builder = new StringBuilder(getClass().getSimpleName());
      builder.append('{');
      appendFieldsToString(builder);
      return builder.append('}').toString();
   }

   @Override
   public ImmortalCacheValue clone() {
      try {
         return (ImmortalCacheValue) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }
   }

   protected void appendFieldsToString(StringBuilder builder) {
      builder.append("value=").append(Util.toStr(value));
      builder.append(", internalMetadata=").append(internalMetadata);
   }
}
