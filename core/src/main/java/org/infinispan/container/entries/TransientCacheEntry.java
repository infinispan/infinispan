package org.infinispan.container.entries;

/**
 * A cache entry that is transient, i.e., it can be considered expired afer a period of not being used
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientCacheEntry extends AbstractInternalCacheEntry {

   long maxIdle = -1;
   long lastUsed;

   TransientCacheEntry() {
      touch();
   }

   TransientCacheEntry(Object key, Object value, long maxIdle) {
      super(key, value);
      this.maxIdle = maxIdle;
      touch();
   }

   TransientCacheEntry(Object key, Object value, long maxIdle, long lastUsed) {
      super(key, value);
      this.maxIdle = maxIdle;
      this.lastUsed = lastUsed;
   }

   public final void touch() {
      lastUsed = System.currentTimeMillis();
   }

   public final boolean canExpire() {
      return true;
   }

   public boolean isExpired() {
      return ExpiryHelper.isExpiredTransient(maxIdle, lastUsed);
   }

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle < 0) {
         return new ImmortalCacheEntry(key, value);
      } else {
         this.maxIdle = maxIdle;
         return this;
      }
   }

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan > -1) {
         TransientMortalCacheEntry tmce = new TransientMortalCacheEntry(key, value);
         tmce.setLifespan(lifespan);
         return tmce;
      } else {
         return this;
      }
   }

   public long getCreated() {
      return -1;
   }

   public final long getLastUsed() {
      return lastUsed;
   }

   public long getLifespan() {
      return -1;
   }

   public long getExpiryTime() {
      return -1;
   }

   public final long getMaxIdle() {
      return maxIdle;
   }

   public InternalCacheValue toInternalCacheValue() {
      return new TransientCacheValue(value, maxIdle, lastUsed);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransientCacheEntry that = (TransientCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;
      if (lastUsed != that.lastUsed) return false;
      if (maxIdle != that.maxIdle) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (int) (lastUsed ^ (lastUsed >>> 32));
      result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
      return result;
   }
}
