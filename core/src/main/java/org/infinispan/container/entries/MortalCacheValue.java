package org.infinispan.container.entries;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A mortal cache value, to correspond with {@link MortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MORTAL_CACHE_VALUE)
public class MortalCacheValue extends ImmortalCacheValue {

   protected long created;
   protected long lifespan;

   public MortalCacheValue(Object value, long created, long lifespan) {
      this(value, null, created, lifespan);
   }

   protected MortalCacheValue(Object value, PrivateMetadata internalMetadata, long created, long lifespan) {
      super(value, internalMetadata);
      this.created = created;
      this.lifespan = lifespan;
   }

   @ProtoFactory
   MortalCacheValue(MarshallableObject<?> wrappedValue, PrivateMetadata internalMetadata, long created,
                    long lifespan) {
      super(wrappedValue, internalMetadata);
      this.created = created;
      this.lifespan =lifespan;
   }

   @Override
   @ProtoField(number = 3, defaultValue = "-1")
   public final long getCreated() {
      return created;
   }

   public final void setCreated(long created) {
      this.created = created;
   }

   @Override
   @ProtoField(number = 4, defaultValue = "-1")
   public final long getLifespan() {
      return lifespan;
   }

   public final void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(lifespan, created, now);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MortalCacheEntry(key, value, internalMetadata, lifespan, created);
   }

   @Override
   public long getExpiryTime() {
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof MortalCacheValue)) {
         return false;
      }
      if (!super.equals(o)) return false;

      MortalCacheValue that = (MortalCacheValue) o;

      return created == that.created && lifespan == that.lifespan;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (created ^ (created >>> 32));
      result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
      return result;
   }

   @Override
   public MortalCacheValue clone() {
      return (MortalCacheValue) super.clone();
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", created=").append(created);
      builder.append(", lifespan=").append(lifespan);
   }
}
