package org.infinispan.container.entries;

/**
 * A cache entry that is both transient and mortal.
 *
 * @author Manik Surtani
 * @since 4.0
 */
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

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan < 0) {
         return new TransientCacheEntry(key, cacheValue.value, cacheValue.lastUsed, cacheValue.maxIdle);
      } else {
         this.cacheValue.lifespan = lifespan;
         return this;
      }
   }

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle < 0) {
         return new MortalCacheEntry(key, cacheValue.value, cacheValue.lifespan, cacheValue.created);
      } else {
         this.cacheValue.maxIdle = maxIdle;
         return this;
      }
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
      return cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
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
}

