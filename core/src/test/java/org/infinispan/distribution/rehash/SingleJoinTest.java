package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.testng.annotations.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

@Test(groups = "functional", testName = "distribution.rehash.SingleJoinTest")
public class SingleJoinTest extends RehashTestBase {
   CacheManager joinerManager;
   Cache<Object, String> joiner;

   void performRehashEvent(boolean offline) {
      joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration);
      joiner = joinerManager.getCache(cacheName);
   }

   void waitForRehashCompletion() {
      // need to block until this join has completed!
      waitForJoinTasksToComplete(SECONDS.toMillis(480), joiner);

      // where does the joiner sit in relation to the other caches?
      int joinerPos = locateJoiner(joinerManager.getAddress());

      log.info("***>>> Joiner is in position " + joinerPos);

      caches.add(joinerPos, joiner);
   }
}
