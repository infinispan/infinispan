package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.AbstractInProcessFuture;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Tests concurrent startup of replicated and distributed caches
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "distribution.ConcurrentStartWithReplTest", groups = "functional")
public class ConcurrentStartWithReplTest extends AbstractInfinispanTest {

   Configuration replCfg, distCfg;

   @BeforeTest
   public void setUp() {
      replCfg = MultipleCacheManagersTest.getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      replCfg.setFetchInMemoryState(true);

      distCfg = MultipleCacheManagersTest.getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      distCfg.setRehashEnabled(true);
   }

   @Test(timeOut = 30000)
   public void testSequence1() throws ExecutionException, InterruptedException {
      /*

      Sequence 1:

         C1 (repl) (becomes coord)
         C2 (repl)
         C1 (dist)
         C2 (dist)

         in the same thread.

       */

      doTest(true, false);

   }

   @Test(timeOut = 30000)
   public void testSequence2() throws ExecutionException, InterruptedException {
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

   @Test(timeOut = 30000)
   public void testSequence3() throws ExecutionException, InterruptedException {
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

   @Test(timeOut = 30000)
   public void testSequence4() throws ExecutionException, InterruptedException {
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
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault());
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault());
      try {
         cm1.defineConfiguration("r", replCfg);
         cm1.defineConfiguration("d", distCfg);
         cm2.defineConfiguration("r", replCfg);
         cm2.defineConfiguration("d", distCfg);

         // first start the repl caches
         Cache<String, String> c1r = startCache(cm1, "r", false).get();
         c1r.put("key", "value");
         Cache<String, String> c2r = startCache(cm2, "r", false).get();
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

   private Future<Cache<String, String>> startCache(final CacheManager cm, final String cacheName, boolean nonBlockingStartup) {
      final Callable<Cache<String, String>> cacheCreator = new Callable<Cache<String, String>>() {

         @Override
         public Cache<String, String> call() throws Exception {
            Cache<String, String> c = cm.getCache(cacheName);
            return c;
         }
      };
      if (nonBlockingStartup) {
         final ExecutorService e = Executors.newFixedThreadPool(1);
         return e.submit(cacheCreator);
      } else {
         return new AbstractInProcessFuture<Cache<String, String>>() {
            @Override
            public Cache<String, String> get() throws InterruptedException, ExecutionException {
               try {
                  return cacheCreator.call();
               } catch (Exception e) {
                  throw new ExecutionException(e);
               }
            }
         };
      }
   }

}


