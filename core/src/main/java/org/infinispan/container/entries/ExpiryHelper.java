package org.infinispan.container.entries;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
class ExpiryHelper {
   static final boolean isExpiredMortal(long lifespan, long created) {
      return lifespan > -1 && System.currentTimeMillis() > created + lifespan;
   }

   static final boolean isExpiredTransient(long maxIdle, long lastUsed) {
      return maxIdle > -1 && System.currentTimeMillis() > maxIdle + lastUsed;
   }

   static final boolean isExpiredTransientMortal(long maxIdle, long lastUsed, long lifespan, long created) {
      return isExpiredTransient(maxIdle, lastUsed) || isExpiredMortal(lifespan, created);
   }
}
