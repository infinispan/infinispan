package org.infinispan.container.entries;

/**
 * A cache entry that is both transient and mortal.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientMortalCacheEntry extends TransientCacheEntry {
   private long created;
   private long lifespan = -1;

   TransientMortalCacheEntry() {
      super();
      created = System.currentTimeMillis();
   }

   TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan) {
      super(key, value, maxIdle);
      created = System.currentTimeMillis();
      this.lifespan = lifespan;
   }

   TransientMortalCacheEntry(Object key, Object value) {
      super(key, value, -1);
      created = System.currentTimeMillis();
   }

   public TransientMortalCacheEntry(Object key, Object value, long maxIdle, long lifespan, long lastUsed, long created) {
      super(key, value, maxIdle, lastUsed);
      this.created = created;
      this.lifespan = lifespan;
   }

   @Override
   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan < 0) {
         TransientCacheEntry tce = new TransientCacheEntry();
         tce.key = key;
         tce.value = value;
         tce.lastUsed = lastUsed;
         tce.maxIdle = maxIdle;
         return tce;
      } else {
         this.lifespan = lifespan;
         return this;
      }
   }

   @Override
   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle < 0) {
         MortalCacheEntry mce = new MortalCacheEntry();
         mce.key = key;
         mce.value = value;
         mce.created = created;
         mce.lifespan = lifespan;
         return mce;
      } else {
         this.maxIdle = maxIdle;
         return this;
      }
   }

   @Override
   public long getLifespan() {
      return lifespan;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public boolean isExpired() {
      return ExpiryHelper.isExpiredTransientMortal(maxIdle, lastUsed, lifespan, created);
   }

   @Override
   public final long getExpiryTime() {
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new TransientMortalCacheValue(value, created, lifespan, maxIdle, lastUsed);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      TransientMortalCacheEntry that = (TransientMortalCacheEntry) o;

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
}

