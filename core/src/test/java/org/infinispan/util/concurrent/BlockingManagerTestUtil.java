package org.infinispan.util.concurrent;

import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;

public class BlockingManagerTestUtil {
   /**
    * Replaces the cache container's {@link BlockingManager} component with one that runs all blocking and non
    * blocking operations in the invoking thread. This is useful for testing when  you want operations to be done
    * sequentially.
    *
    * @param cacheContainer Container of which the blocking manager is to be replaced
    * @return The original BlockingManager that was configured in the container
    */
   public static BlockingManager replaceBlockingManagerWithInline(CacheContainer cacheContainer) {
      BlockingManagerImpl replacement = new BlockingManagerImpl();
      BlockingManager prev = TestingUtil.replaceComponent(cacheContainer, BlockingManager.class, replacement, true);
      replacement.blockingExecutor = new WithinThreadExecutor();
      replacement.nonBlockingExecutor = new WithinThreadExecutor();
      replacement.start();
      return prev;
   }
}
