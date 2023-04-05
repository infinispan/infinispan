package org.infinispan.util.concurrent;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;

public class BlockingManagerTestUtil {
   /**
    * Replaces the cache container's {@link BlockingManager} and {@link NonBlockingManager} components with ones that
    * runs all blocking and non blocking operations in the invoking thread. This is useful for testing when  you want
    * operations to be done sequentially.
    * <p>
    * This operation will be undone if component registry is rewired
    *
    * @param cacheContainer Container of which the blocking manager is to be replaced
    */
   public static void replaceManagersWithInline(CacheContainer cacheContainer) {
      NonBlockingManagerImpl nonBlockingManager = (NonBlockingManagerImpl) TestingUtil.extractGlobalComponent(cacheContainer, NonBlockingManager.class);
      nonBlockingManager.executor = new WithinThreadExecutor();

      BlockingManagerImpl manager = (BlockingManagerImpl) TestingUtil.extractGlobalComponent(cacheContainer, BlockingManager.class);
      manager.blockingExecutor = new WithinThreadExecutor();
      manager.nonBlockingExecutor = new WithinThreadExecutor();
      manager.nonBlockingManager = nonBlockingManager;
   }
}
