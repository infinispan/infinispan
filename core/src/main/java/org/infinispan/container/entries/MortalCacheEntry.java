package org.infinispan.container.entries;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshalls;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheEntry extends AbstractInternalCacheEntry {
   private MortalCacheValue cacheValue;

   public Object getValue() {
      return cacheValue.value;
   }

   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   MortalCacheEntry(Object key, Object value, long lifespan) {
      this(key, value, lifespan, System.currentTimeMillis());
   }

   MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key);
      cacheValue = new MortalCacheValue(value, created, lifespan);
   }

   public final boolean isExpired() {
      return ExpiryHelper.isExpiredMortal(cacheValue.lifespan, cacheValue.created);
   }

   public final boolean canExpire() {
      return true;
   }

   public void setLifespan(long lifespan) {
      cacheValue.setLifespan(lifespan);
   }

   public final long getCreated() {
      return cacheValue.created;
   }

   public final long getLastUsed() {
      return -1;
   }

   public final long getLifespan() {
      return cacheValue.lifespan;
   }

   public final long getMaxIdle() {
      return -1;
   }

   public final long getExpiryTime() {
      return cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
   }

   public final void touch() {
      // no-op
   }

   public final void reincarnate() {
      cacheValue.created = System.currentTimeMillis();
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MortalCacheEntry that = (MortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue.value != null ? !cacheValue.value.equals(that.cacheValue.value) : that.cacheValue.value != null)
         return false;
      if (cacheValue.created != that.cacheValue.created) return false;
      return cacheValue.lifespan == that.cacheValue.lifespan;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue.value != null ? cacheValue.value.hashCode() : 0);
      result = 31 * result + (int) (cacheValue.created ^ (cacheValue.created >>> 32));
      result = 31 * result + (int) (cacheValue.lifespan ^ (cacheValue.lifespan >>> 32));
      return result;
   }

   @Override
   public MortalCacheEntry clone() {
      MortalCacheEntry clone = (MortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   @Marshalls(typeClasses = MortalCacheEntry.class, id = Ids.MORTAL_ENTRY)
   public static class Externalizer implements org.infinispan.marshall.Externalizer<MortalCacheEntry> {
      public void writeObject(ObjectOutput output, MortalCacheEntry mce) throws IOException {
         output.writeObject(mce.key);
         output.writeObject(mce.cacheValue.value);
         UnsignedNumeric.writeUnsignedLong(output, mce.cacheValue.created);
         output.writeLong(mce.cacheValue.lifespan); // could be negative so should not use unsigned longs
      }

      public MortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         return new MortalCacheEntry(k, v, lifespan, created);
      }      
   }

   @Override
   public String toString() {
      return "MortalCacheEntry{" +
            "cacheValue=" + cacheValue +
            "} " + super.toString();
   }
}
