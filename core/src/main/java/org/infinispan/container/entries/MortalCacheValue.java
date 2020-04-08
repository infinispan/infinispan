package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A mortal cache value, to correspond with {@link MortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
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

   @Override
   public final long getCreated() {
      return created;
   }

   public final void setCreated(long created) {
      this.created = created;
   }

   @Override
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

   public static class Externalizer extends AbstractExternalizer<MortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MortalCacheValue mcv) throws IOException {
         output.writeObject(mcv.value);
         output.writeObject(mcv.internalMetadata);
         UnsignedNumeric.writeUnsignedLong(output, mcv.created);
         output.writeLong(mcv.lifespan); // could be negative so should not use unsigned longs
      }

      @Override
      public MortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lifespan = input.readLong();
         return new MortalCacheValue(value, internalMetadata, created, lifespan);
      }

      @Override
      public Integer getId() {
         return Ids.MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MortalCacheValue>> getTypeClasses() {
         return Collections.singleton(MortalCacheValue.class);
      }
   }
}
