package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(testName = "distribution.RehashJoinTest", groups = "functional")
public class RehashJoinTest extends BaseDistFunctionalTest {

   Log log = LogFactory.getLog(RehashJoinTest.class);
   CacheManager joinerManager;

   public RehashJoinTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration);
   }

   public void testRehashOnJoin() {
      MagicKey k1 = new MagicKey(c1, "k1");
      MagicKey k2 = new MagicKey(c2, "k2");
      MagicKey k3 = new MagicKey(c3, "k3");
      MagicKey k4 = new MagicKey(c4, "k4");

      List<MagicKey> keys = new ArrayList<MagicKey>(4);
      keys.add(k1);
      keys.add(k2);
      keys.add(k3);
      keys.add(k4);

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      log.info("***>>> Firing up new joiner!");

      // now fire up a new joiner
      Cache<Object, String> joiner = joinerManager.getCache(cacheName);

      // need to wait for the joiner to, well, join.
      TestingUtil.blockUntilViewsReceived(SECONDS.toMillis(480), cacheManagers.toArray(new CacheManager[cacheManagers.size()]));

      // need to block until this join has completed!
      waitForJoinTasksToComplete(SECONDS.toMillis(480), joiner);

      // where does the joiner sit in relation to the other caches?
      int joinerPos = locateJoiner(joinerManager.getAddress());

      log.info("***>>> Joiner is in position " + joinerPos);

      caches.add(joinerPos, joiner);
      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      assertProperConsistentHashOnAllCaches();
   }

   public void testMultipleJoiners() throws InterruptedException {

      // have all JOIN phases completed?
      for (Cache c : caches) {
         DistributionManagerImpl dmi = (DistributionManagerImpl) getDistributionManager(c);
         assert !dmi.rehashInProgress : "Cache " + addressOf(c) + " still has rehashInProgress=true!";
         assert dmi.rehashQueue.isEmpty() : "Cache " + addressOf(c) + " still has queued RehashTasks!";
         assert !(dmi.getConsistentHash() instanceof UnionConsistentHash) : "Cache " + addressOf(c) + " still using a UnionConsistentHash!";
      }

      MagicKey k1 = new MagicKey(c1, "k1");
      MagicKey k2 = new MagicKey(c2, "k2");
      MagicKey k3 = new MagicKey(c3, "k3");
      MagicKey k4 = new MagicKey(c4, "k4");

      List<MagicKey> keys = new ArrayList<MagicKey>(4);
      keys.add(k1);
      keys.add(k2);
      keys.add(k3);
      keys.add(k4);

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      int numNewJoiners = 3; // in addition to the 1 joiner defined in createCacheManagers()!
      int numJoiners = numNewJoiners + 1;
      final CacheManager[] joinerManagers = new CacheManager[numJoiners];
      final Cache[] joiners = new Cache[numJoiners];
      joinerManagers[0] = joinerManager;

      for (i = 1; i < numJoiners; i++) {
         joinerManagers[i] = addClusterEnabledCacheManager();
         joinerManagers[i].defineConfiguration(cacheName, configuration);
      }

      log.info("***>>> Firing up {0} new joiners!", numJoiners);

      // now fire up a new joiners, in separate threads.
      final CountDownLatch joinLatch = new CountDownLatch(1);
      Thread[] threads = new Thread[numJoiners];

      for (i = 0; i < numJoiners; i++) {
         final int idx = i;
         threads[idx] = new Thread() {
            public void run() {
               try {
                  joinLatch.await();
               } catch (InterruptedException e) {
                  log.error(e);
               }
               joiners[idx] = joinerManagers[idx].getCache(cacheName);
            }
         };
         threads[idx].start();
      }

      joinLatch.countDown();

      for (Thread t : threads) t.join();

      CacheManager[] cacheManagerArray = cacheManagers.toArray(new CacheManager[cacheManagers.size()]);
      log.info("Number of cache manager views to wait for: {0}", cacheManagerArray.length);

      // need to wait for the joiner to, well, join.
      TestingUtil.blockUntilViewsReceived(SECONDS.toMillis(480), cacheManagerArray);
      // where do the joiners sit in relation to the other caches?


      waitForJoinTasksToComplete(SECONDS.toMillis(480), joiners);

      // need to wait a *short while* before we attempt to locate joiners, since post-join invalidation messages are sent async.
      // TODO replace this with some form of command detection on remote nodes.
      // join tasks happen sequentially as well, so this needs some time to finish
      TestingUtil.sleepThread(SECONDS.toMillis(2));

      int[] joinersPos = new int[numJoiners];
      for (i = 0; i < numJoiners; i++) joinersPos[i] = locateJoiner(joinerManagers[i].getAddress());

      log.info("***>>> Joiners are in positions " + Arrays.toString(joinersPos));
      for (i = 0; i < numJoiners; i++) {
         if (joinersPos[i] > caches.size())
            caches.add(joiners[i]);
         else
            caches.add(joinersPos[i], joiners[i]);
      }
      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      assertProperConsistentHashOnAllCaches();
   }

   private int locateJoiner(Address joinerAddress) {
      for (Cache c : Arrays.asList(c1, c2, c3, c4)) {
         DefaultConsistentHash dch = getDefaultConsistentHash(c, SECONDS.toMillis(480));
         int i = 0;
         for (Address a : dch.positions.values()) {
            if (a.equals(joinerAddress)) return i;
            i++;
         }
      }
      throw new RuntimeException("Cannot locate joiner! Joiner is [" + joinerAddress + "]");
   }
}
