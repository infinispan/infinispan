package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

/**
 * A mortal cache value, to correspond with {@link org.infinispan.container.entries.MortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = MortalCacheValue.Externalizer.class, id = Ids.MORTAL_VALUE)
public class MortalCacheValue extends ImmortalCacheValue {

   long created;
   long lifespan = -1;

   MortalCacheValue(Object value, long created, long lifespan) {
      super(value);
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
   public boolean isExpired() {
      return ExpiryHelper.isExpiredMortal(lifespan, created);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MortalCacheEntry(key, value, lifespan, created);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MortalCacheValue)) return false;
      if (!super.equals(o)) return false;

      MortalCacheValue that = (MortalCacheValue) o;

      if (created != that.created) return false;
      if (lifespan != that.lifespan) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (created ^ (created >>> 32));
      result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "MortalCacheValue{" +
            "created=" + created +
            ", lifespan=" + lifespan +
            "} " + super.toString();
   }

   @Override
   public MortalCacheValue clone() {
      return (MortalCacheValue) super.clone();
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         MortalCacheValue icv = (MortalCacheValue) subject;
         output.writeObject(icv.value);
         UnsignedNumeric.writeUnsignedLong(output, icv.created);
         output.writeLong(icv.lifespan); // could be negative so should not use unsigned longs
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new MortalCacheValue(v, created, lifespan);
      }
   }
}
