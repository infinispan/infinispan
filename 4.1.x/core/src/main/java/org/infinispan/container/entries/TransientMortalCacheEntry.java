package org.infinispan.container.entries;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static java.lang.Math.min;

/**
 * A cache entry that is both transient and mortal.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = TransientMortalCacheEntry.Externalizer.class, id = Ids.TRANSIENT_MORTAL_ENTRY)
public class TransientMortalCacheEntry extends AbstractInternalCacheEntry {

   private TransientMortalCacheValue cacheValue;

   TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan) {
      super(key);
      cacheValue = new TransientMortalCacheValue(value, System.currentTimeMillis(), lifespan, maxIdle);
      touch();
   }

   TransientMortalCacheEntry(Object key, Object value) {
      super(key);
      cacheValue = new TransientMortalCacheValue(value, System.currentTimeMillis());
      touch();
   }

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long lastUsed, long created) {
      super(key);
      this.cacheValue = new TransientMortalCacheValue(value, created, lifespan, maxIdle, lastUsed);
   }

   public void setLifespan(long lifespan) {
      this.cacheValue.lifespan = lifespan;
   }

   public void setMaxIdle(long maxIdle) {
      this.cacheValue.maxIdle = maxIdle;
   }

   public Object getValue() {
      return cacheValue.value;
   }

   public long getLifespan() {
      return cacheValue.lifespan;
   }

   public final boolean canExpire() {
      return true;
   }

   public long getCreated() {
      return cacheValue.created;
   }

   public boolean isExpired() {
      return cacheValue.isExpired();
   }

   public final long getExpiryTime() {
      long lset = cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
      long muet = cacheValue.maxIdle > -1 ? cacheValue.lastUsed + cacheValue.maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   public long getLastUsed() {
      return cacheValue.lastUsed;
   }

   public final void touch() {
      cacheValue.lastUsed = System.currentTimeMillis();
   }

   public final void reincarnate() {
      cacheValue.created = System.currentTimeMillis();
   }   

   public long getMaxIdle() {
      return cacheValue.maxIdle;
   }

   public Object setValue(Object value) {
      return cacheValue.maxIdle;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransientMortalCacheEntry that = (TransientMortalCacheEntry) o;

      if (cacheValue.created != that.cacheValue.created) return false;
      if (cacheValue.lifespan != that.cacheValue.lifespan) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (cacheValue.created ^ (cacheValue.created >>> 32));
      result = 31 * result + (int) (cacheValue.lifespan ^ (cacheValue.lifespan >>> 32));
      return result;
   }

   @Override
   public TransientMortalCacheEntry clone() {
      TransientMortalCacheEntry clone = (TransientMortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "cacheValue=" + cacheValue +
            "} " + super.toString();
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         TransientMortalCacheEntry ice = (TransientMortalCacheEntry) subject;
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.created);
         output.writeLong(ice.cacheValue.lifespan); // could be negative so should not use unsigned longs
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.lastUsed);
         output.writeLong(ice.cacheValue.maxIdle); // could be negative so should not use unsigned longs
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         Long lifespan = input.readLong();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new TransientMortalCacheEntry(k, v, maxIdle, lifespan, lastUsed, created);
      }
   }
}

