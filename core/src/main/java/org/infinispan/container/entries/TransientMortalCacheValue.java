package org.infinispan.container.entries;

/**
 * A transient, mortal cache value to correspond with {@link org.infinispan.container.entries.TransientMortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientMortalCacheValue extends MortalCacheValue {
   long maxIdle = -1;
   long lastUsed;

   TransientMortalCacheValue(Object value, long created, long lifespan, long maxIdle, long lastUsed) {
      this(value, created, lifespan, maxIdle);
      this.lastUsed = lastUsed;
   }

   public TransientMortalCacheValue(Object value, long created, long lifespan, long maxIdle) {
      super(value, created, lifespan);
      this.maxIdle = maxIdle;
   }

   public TransientMortalCacheValue(Object value, long created) {
      super(value, created, -1);
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

   public boolean isExpired() {
      return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created);
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TransientMortalCacheValue)) return false;
      if (!super.equals(o)) return false;

      TransientMortalCacheValue that = (TransientMortalCacheValue) o;

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
      return "TransientMortalCacheValue{" +
            "maxIdle=" + maxIdle +
            ", lastUsed=" + lastUsed +
            "} " + super.toString();
   }
}
