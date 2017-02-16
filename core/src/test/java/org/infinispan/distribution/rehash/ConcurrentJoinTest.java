package org.infinispan.distribution.rehash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.ConcurrentJoinTest", description = "See ISPN-1123")
public class ConcurrentJoinTest extends RehashTestBase {

   List<EmbeddedCacheManager> joinerManagers;
   List<Cache<Object, String>> joiners;

   static final int NUM_JOINERS = 4;

   void performRehashEvent(boolean offline) throws Exception {
      joinerManagers = new CopyOnWriteArrayList<EmbeddedCacheManager>();
      joiners = new CopyOnWriteArrayList<Cache<Object, String>>(new Cache[NUM_JOINERS]);

      for (int i = 0; i < NUM_JOINERS; i++) {
         EmbeddedCacheManager joinerManager = addClusterEnabledCacheManager(new TransportFlags().withFD(false).withPortRange(i));
         joinerManager.defineConfiguration(cacheName, configuration.build());
         joinerManagers.add(joinerManager);
         joiners.set(i, null);
      }

      Future<?>[] threads = new Future[NUM_JOINERS];
      for (int i = 0; i < NUM_JOINERS; i++) {
         final int ii = i;
         threads[i] = fork(() -> {
               EmbeddedCacheManager joinerManager = joinerManagers.get(ii);
               Cache<Object, String> joiner = joinerManager.getCache(cacheName);
               joiners.set(ii, joiner);
         });
      }

      for (int i = 0; i < NUM_JOINERS; i++) {
         threads[i].get(30, TimeUnit.SECONDS);
      }
   }

   @SuppressWarnings("unchecked")
   void waitForRehashCompletion() {
      List<CacheContainer> allCacheManagers = new ArrayList<CacheContainer>(cacheManagers);
      // Collection already contains all cache managers, no need to add more
      TestingUtil.blockUntilViewsReceived(60000, false, allCacheManagers);
      waitForClusterToForm(cacheName);
      for (int i = 0; i < NUM_JOINERS; i++) {
         caches.add(joiners.get(i));
      }
   }
}
