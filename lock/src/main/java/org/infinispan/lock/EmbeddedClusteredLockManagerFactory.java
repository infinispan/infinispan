package org.infinispan.lock;

import static java.util.Objects.requireNonNull;

import org.infinispan.commons.util.Experimental;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A {@link ClusteredLockManager} factory for embedded cached.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Experimental
public final class EmbeddedClusteredLockManagerFactory {

   private EmbeddedClusteredLockManagerFactory() {
   }

   public static ClusteredLockManager from(EmbeddedCacheManager cacheManager) {
      return requireNonNull(cacheManager, "EmbeddedCacheManager can't be null.")
            .getGlobalComponentRegistry()
            .getComponent(ClusteredLockManager.class);
   }
}
