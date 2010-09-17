package org.infinispan.container.entries;

/**
 * Provide utility methods for dealing with expiration of cache entries.
 *
 * @author Manik Surtani
 * @since 4.0
 */
class ExpiryHelper {
   static boolean isExpiredMortal(long lifespan, long created) {
      return lifespan > -1 && created > -1 && System.currentTimeMillis() > created + lifespan;
   }

   static boolean isExpiredTransient(long maxIdle, long lastUsed) {
      return maxIdle > -1 && lastUsed > -1 && System.currentTimeMillis() > maxIdle + lastUsed;
   }

   static boolean isExpiredTransientMortal(long maxIdle, long lastUsed, long lifespan, long created) {
      return isExpiredTransient(maxIdle, lastUsed) || isExpiredMortal(lifespan, created);
   }
}
