package org.infinispan.distribution;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.concurrent.CompletableFutures;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup of replicated and distributed caches
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "distribution.ConcurrentStartWithReplTest", groups = "functional")
public class ConcurrentStartWithReplTest extends AbstractInfinispanTest {

   private ConfigurationBuilder replCfg, distCfg;

   @BeforeTest
   public void setUp() {
      replCfg = MultipleCacheManagersTest.getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      replCfg.clustering().stateTransfer().fetchInMemoryState(true);

      distCfg = MultipleCacheManagersTest.getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      distCfg.clustering().stateTransfer().fetchInMemoryState(true);
   }

   @Test(timeOut = 60000)
   public void testSequence1() throws ExecutionException, InterruptedException {
      TestResourceTracker.testThreadStarted(this);
      /*

      Sequence 1:

         C1 (repl) (becomes coord)
         C2 (dist)
         C1 (repl)
         C2 (dist)

         in the same thread.

       */

      doTest(true, false);

   }

   @Test(timeOut = 60000)
   public void testSequence2() throws ExecutionException, InterruptedException {
      TestResourceTracker.testThreadStarted(this);
      /*

      Sequence 2:

         C1 (repl) (becomes coord)
         C2 (repl)
         C2 (dist)
         C1 (dist)

         in the same thread.

       */

      doTest(false, false);
   }

   @Test(timeOut = 60000)
   public void testSequence3() throws ExecutionException, InterruptedException {
      TestResourceTracker.testThreadStarted(this);
      /*

      Sequence 3:

         C1 (repl) (becomes coord)
         C2 (repl)
         C1 (dist) (async thread)
         C2 (dist) (async thread)

         in the same thread, except the last two which are in separate threads

       */
      doTest(true, true);
   }

   @Test(timeOut = 60000)
   public void testSequence4() throws ExecutionException, InterruptedException {
      TestResourceTracker.testThreadStarted(this);
      /*

      Sequence 4:

         C1 (repl) (becomes coord)
         C2 (repl)
         C2 (dist) (async thread)
         C1 (dist) (async thread)

         in the same thread, except the last two which are in separate threads

       */
      doTest(false, true);
   }

   private void doTest(boolean inOrder, boolean nonBlockingStartupForDist) throws ExecutionException, InterruptedException {
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(new ConfigurationBuilder());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(new ConfigurationBuilder());
      try {
         cm1.defineConfiguration("r", replCfg.build());
         cm1.defineConfiguration("d", distCfg.build());
         cm2.defineConfiguration("r", replCfg.build());
         cm2.defineConfiguration("d", distCfg.build());

         // first start the repl caches
         Cache<String, String> c1r = startCache(cm1, "r", false).get();
         c1r.put("key", "value");
         Cache<String, String> c2r = startCache(cm2, "r", false).get();
         TestingUtil.blockUntilViewsReceived(10000, c1r, c2r);
         TestingUtil.waitForStableTopology(c1r, c2r);
         assert "value".equals(c2r.get("key"));

         // now the dist ones
         Future<Cache<String, String>> c1df = startCache(inOrder ? cm1 : cm2, "d", nonBlockingStartupForDist);
         Future<Cache<String, String>> c2df = startCache(inOrder ? cm2 : cm1, "d", nonBlockingStartupForDist);
         Cache<String, String> c1d = c1df.get();
         Cache<String, String> c2d = c2df.get();

         c1d.put("key", "value");
         assert "value".equals(c2d.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

   private Future<Cache<String, String>> startCache(final CacheContainer cm, final String cacheName, boolean nonBlockingStartup) {
      final Callable<Cache<String, String>> cacheCreator = () -> cm.getCache(cacheName);
      if (nonBlockingStartup) {
         return fork(cacheCreator);
      } else {
         try {
            Cache<String, String> cache = cacheCreator.call();
            return CompletableFuture.completedFuture(cache);
         } catch (Exception e) {
            return CompletableFutures.completedExceptionFuture(e);
         }
      }
   }

}
