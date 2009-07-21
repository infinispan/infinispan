package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;

/**
 * A transient cache value, to correspond with {@link org.infinispan.container.entries.TransientCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = TransientCacheValue.Externalizer.class, id = Ids.TRANSIENT_VALUE)
public class TransientCacheValue extends ImmortalCacheValue {
   long maxIdle = -1;
   long lastUsed;

   TransientCacheValue(Object value, long maxIdle, long lastUsed) {
      super(value);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   @Override
   public long getMaxIdle() {
      return maxIdle;
   }

   public void setMaxIdle(long maxIdle) {
      this.maxIdle = maxIdle;
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   public void setLastUsed(long lastUsed) {
      this.lastUsed = lastUsed;
   }

   @Override
   public final boolean isExpired() {
      return ExpiryHelper.isExpiredTransient(maxIdle, lastUsed);
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new TransientCacheEntry(key, value, maxIdle, lastUsed);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TransientCacheValue)) return false;
      if (!super.equals(o)) return false;

      TransientCacheValue that = (TransientCacheValue) o;

      if (lastUsed != that.lastUsed) return false;
      if (maxIdle != that.maxIdle) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "TransientCacheValue{" +
            "maxIdle=" + maxIdle +
            ", lastUsed=" + lastUsed +
            "} " + super.toString();
   }

   @Override
   public TransientCacheValue clone() {
      return (TransientCacheValue) super.clone();
   }
   
   public static class Externalizer implements org.infinispan.marshall.Externalizer {
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         TransientCacheValue icv = (TransientCacheValue) subject;
         output.writeObject(icv.value);
         UnsignedNumeric.writeUnsignedLong(output, icv.lastUsed);
         output.writeLong(icv.maxIdle); // could be negative so should not use unsigned longs
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         Long maxIdle = input.readLong();
         return new TransientCacheValue(v, maxIdle, lastUsed);
      }      
   }
}
