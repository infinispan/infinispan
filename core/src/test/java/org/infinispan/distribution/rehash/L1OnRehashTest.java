package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.CacheManager;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Ensures entries are moved to L1 if they are removed due to a rehash
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "distribution.rehash.L1OnRehashTest")
public class L1OnRehashTest extends BaseDistFunctionalTest {
   public L1OnRehashTest() {
      this.tx = false;
      this.sync = true;
      this.l1CacheEnabled = true;
      this.performRehashing = true;
      this.l1OnRehash = true;
      this.INIT_CLUSTER_SIZE = 2;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   CacheManager joinerManager;
   Cache<Object, String> joiner;

   void performRehashEvent() {
      joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration);
      joiner = joinerManager.getCache(cacheName);
   }

   int waitForRehashCompletion() {
      // need to block until this join has completed!
      waitForJoinTasksToComplete(SECONDS.toMillis(480), joiner);

      // where does the joiner sit in relation to the other caches?
      int joinerPos = locateJoiner(joinerManager.getAddress());

      log.info("***>>> Joiner is in position " + joinerPos);

      caches.add(joinerPos, joiner);
      return joinerPos;
   }

   private List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey(c1, "k1"), new MagicKey(c2, "k2")
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      log.info("Initialized with keys {0}", keys);
      return keys;
   }

   public void testInvalidationBehaviorOnRehash() {
      // start with 2 caches...
      List<MagicKey> keys = init();
      System.out.println("Old CH positions are " + getConsistentHash(c1));
      // add 1
      performRehashEvent();
      int joinerPos = waitForRehashCompletion();

      // now where is joiner in relation to the other 2?
      // we can have either
      // 1. J, C1, C2
      // 2. C1, J, C2
      // 3. C1, C2, J
      // for the purpose of CH, 1 == 3.

      System.out.println("New CH positions are " + getConsistentHash(c1));
      // invalidations happen asynchronously!  :(
      TestingUtil.sleepThread(2000);

      Cache<Object, String> cacheToCheckForInvalidation = joinerPos + 1 == caches.size() ? caches.get(0) : caches.get(joinerPos + 1);
      MagicKey rehashedKey = keys.get(joinerPos == 1 ? 0 : 1);
      if (l1OnRehash)
         assertIsInL1(cacheToCheckForInvalidation, rehashedKey);
      else
         assertIsNotInL1(cacheToCheckForInvalidation, rehashedKey);

      assertIsInContainerImmortal(joiner, rehashedKey);
   }
}
