package org.infinispan.container.entries;

/**
 * A transient cache value, to correspond with {@link org.infinispan.container.entries.TransientCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
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
   public boolean isExpired() {
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
}
