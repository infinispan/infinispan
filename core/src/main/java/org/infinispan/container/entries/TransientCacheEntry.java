package org.infinispan.container.entries;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static java.lang.Math.min;

/**
 * A cache entry that is transient, i.e., it can be considered expired afer a period of not being used.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = TransientCacheEntry.Externalizer.class, id = Ids.TRANSIENT_ENTRY)
public class TransientCacheEntry extends AbstractInternalCacheEntry {
   private TransientCacheValue cacheValue;

   TransientCacheEntry(Object key, Object value, long maxIdle) {
      this(key, value, maxIdle, System.currentTimeMillis());
   }

   TransientCacheEntry(Object key, Object value, long maxIdle, long lastUsed) {
      super(key);
      cacheValue = new TransientCacheValue(value, maxIdle, lastUsed);
   }

   public Object getValue() {
      return cacheValue.value;
   }

   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   public final void touch() {
      cacheValue.lastUsed = System.currentTimeMillis();
   }

   public final boolean canExpire() {
      return true;
   }

   public boolean isExpired() {
      return cacheValue.isExpired();
   }

   public void setMaxIdle(long maxIdle) {
      cacheValue.maxIdle = maxIdle;
   }

   public long getCreated() {
      return -1;
   }

   public final long getLastUsed() {
      return cacheValue.lastUsed;
   }

   public long getLifespan() {
      return -1;
   }

   public long getExpiryTime() {
      return cacheValue.maxIdle > -1 ? cacheValue.lastUsed + cacheValue.maxIdle : -1;
   }

   public final long getMaxIdle() {
      return cacheValue.maxIdle;
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransientCacheEntry that = (TransientCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue.value != null ? !cacheValue.value.equals(that.cacheValue.value) : that.cacheValue.value != null)
         return false;
      if (cacheValue.lastUsed != that.cacheValue.lastUsed) return false;
      if (cacheValue.maxIdle != that.cacheValue.maxIdle) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue.value != null ? cacheValue.value.hashCode() : 0);
      result = 31 * result + (int) (cacheValue.lastUsed ^ (cacheValue.lastUsed >>> 32));
      result = 31 * result + (int) (cacheValue.maxIdle ^ (cacheValue.maxIdle >>> 32));
      return result;
   }

   @Override
   public TransientCacheEntry clone() {
      TransientCacheEntry clone = (TransientCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         TransientCacheEntry ice = (TransientCacheEntry) subject;
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);
         UnsignedNumeric.writeUnsignedLong(output, ice.cacheValue.lastUsed);
         output.writeLong(ice.cacheValue.maxIdle); // could be negative so should not use unsigned longs
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new TransientCacheEntry(k, v, maxIdle, lastUsed);
      }
   }

   @Override
   public String toString() {
      return "TransientCacheEntry{" +
            "cacheValue=" + cacheValue +
            "} " + super.toString();
   }
}
