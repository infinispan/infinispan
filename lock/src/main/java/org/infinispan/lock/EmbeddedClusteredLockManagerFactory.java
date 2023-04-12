package org.infinispan.lock;

import static java.util.Objects.requireNonNull;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.lock.logging.Log;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;

/**
 * A {@link ClusteredLockManager} factory for embedded caches.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public final class EmbeddedClusteredLockManagerFactory {

   private static final Log log = LogFactory.getLog(EmbeddedClusteredLockManagerFactory.class, Log.class);

   private EmbeddedClusteredLockManagerFactory() {
   }

   public static ClusteredLockManager from(EmbeddedCacheManager cacheManager) {
      requireNonNull(cacheManager, "EmbeddedCacheManager can't be null.");

      if (!cacheManager.getCacheManagerConfiguration().isClustered()) {
         throw log.requireClustered();
      }
      return SecurityActions.getGlobalComponentRegistry(cacheManager)
            .getComponent(ClusteredLockManager.class);
   }
}
