package org.infinispan.container.entries;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A cache entry that is transient, i.e., it can be considered expired afer a period of not being used.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class TransientCacheEntry extends AbstractInternalCacheEntry {
   private static final Log log = LogFactory.getLog(TransientCacheEntry.class);
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

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle < 0) {
         if (log.isTraceEnabled()) log.trace("Converting {0} into an inmortal cache entry", this);
         return new ImmortalCacheEntry(key, cacheValue.value);
      } else {
         cacheValue.maxIdle = maxIdle;
         return this;
      }
   }

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan > -1) {
         TransientMortalCacheEntry tmce = new TransientMortalCacheEntry(key, cacheValue.value);
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
      return cacheValue.lastUsed;
   }

   public long getLifespan() {
      return -1;
   }

   public long getExpiryTime() {
      return -1;
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

}
