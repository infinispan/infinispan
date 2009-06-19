package org.infinispan.container.entries;

/**
 * A factory for internal entries
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class InternalEntryFactory {
   public static final InternalCacheEntry create(Object key, Object value) {
      return new ImmortalCacheEntry(key, value);
   }

   public static final InternalCacheEntry create(Object key, Object value, long lifespan) {
      return lifespan > -1 ? new MortalCacheEntry(key, value, lifespan) : new ImmortalCacheEntry(key, value);
   }

   public static final InternalCacheEntry create(Object key, Object value, long lifespan, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle);
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan);
   }

   public static final InternalCacheEntry create(Object key, Object value, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, created);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, lastUsed);
      return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
   }

   public static final InternalCacheValue createValue(Object v) {
      return new ImmortalCacheValue(v);
   }

   public static final InternalCacheValue createValue(Object v, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(v);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(v, created, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(v, maxIdle, lastUsed);
      return new TransientMortalCacheValue(v, maxIdle, lifespan, lastUsed, created);
   }
}
