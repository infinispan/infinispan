package org.infinispan.manager;

/**
 * Let tests access package-protected methods in DefaultCacheManager
 */
public class DefaultCacheManagerHelper {
   public static void enableManagerGetCacheBlockingCheck() {
      // Require isRunning(cache) before getCache(name) on non-blocking threads
      DefaultCacheManager.enableGetCacheBlockingCheck();
   }
}
