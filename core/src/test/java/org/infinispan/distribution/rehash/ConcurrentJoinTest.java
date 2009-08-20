package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentJoinTest")
public class ConcurrentJoinTest extends RehashTestBase {

   List<CacheManager> joinerManagers;
   List<Cache<Object, String>> joiners;

   final int numJoiners = 4;

   void performRehashEvent() {
      joinerManagers = new ArrayList<CacheManager>(numJoiners);
      joiners = new ArrayList<Cache<Object, String>>(numJoiners);
      for (int i = 0; i < numJoiners; i++) {
         CacheManager joinerManager = addClusterEnabledCacheManager();
         joinerManager.defineConfiguration(cacheName, configuration);
         Cache<Object, String> joiner = joinerManager.getCache(cacheName);
         joinerManagers.add(joinerManager);
         joiners.add(joiner);
      }
   }

   @SuppressWarnings("unchecked")
   void waitForRehashCompletion() {
      waitForJoinTasksToComplete(SECONDS.toMillis(480), joiners.toArray(new Cache[numJoiners]));
      TestingUtil.sleepThread(SECONDS.toMillis(2));
      int[] joinersPos = new int[numJoiners];
      for (int i = 0; i < numJoiners; i++) joinersPos[i] = locateJoiner(joinerManagers.get(i).getAddress());

      log.info("***>>> Joiners are in positions " + Arrays.toString(joinersPos));
      for (int i = 0; i < numJoiners; i++) {
         if (joinersPos[i] > caches.size())
            caches.add(joiners.get(i));
         else
            caches.add(joinersPos[i], joiners.get(i));
      }
   }
}
