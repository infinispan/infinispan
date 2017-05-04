package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests data loss during state transfer when the originator of a put operation becomes the primary owner of the
 * modified key. See https://issues.jboss.org/browse/ISPN-3366
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxOriginatorBecomingPrimaryOwnerTest")
@CleanupAfterMethod
public class NonTxOriginatorBecomingPrimaryOwnerTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   public void testPrimaryOwnerLeavingDuringPut() throws Exception {
      doTest(false);
   }

   public void testPrimaryOwnerLeavingDuringPutIfAbsent() throws Exception {
      doTest(true);
   }

   private void doTest(final boolean conditional) throws Exception {
      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      AdvancedCache<Object, Object> cache1 = advancedCache(1);
      AdvancedCache<Object, Object> cache2 = advancedCache(2);

      // Every PutKeyValueCommand will be blocked before reaching the distribution interceptor
      CyclicBarrier distInterceptorBarrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor = new BlockingInterceptor<>(distInterceptorBarrier, PutKeyValueCommand.class, false, false);
      cache0.getAsyncInterceptorChain().addInterceptorBefore(blockingInterceptor, TriangleDistributionInterceptor.class);

      for (int i = 0; i < NUM_KEYS; i++) {
         // Try to put a key/value from cache0 with cache1 the primary owner
         final MagicKey key = new MagicKey("key" + i, cache1);
         Future<Object> future = fork(() -> conditional ? cache0.putIfAbsent(key, "v") : cache0.put(key, "v"));

         // Wait for the put command to pass through EntryWrappingInterceptor
         distInterceptorBarrier.await(10, TimeUnit.SECONDS);

         // Stop blocking new commands, to allow state transfer to finish
         blockingInterceptor.suspend(true);

         // Kill cache1
         cache1.stop();

         // Wait for the new topology to be installed
         TestingUtil.waitForNoRebalance(cache0, cache2);

         // Resume blocking new commands
         blockingInterceptor.suspend(false);

         // Unblock the command
         distInterceptorBarrier.await(10, TimeUnit.SECONDS);

         // StateTransferInterceptor retries the command, and it should block again in BlockingInterceptor.
         distInterceptorBarrier.await(10, TimeUnit.SECONDS);
         distInterceptorBarrier.await(10, TimeUnit.SECONDS);

         if (cache2.getAdvancedCache().getDistributionManager().getPrimaryLocation(key).equals(address(2))) {
            // cache2 forwards the command back to cache0, blocking again
            distInterceptorBarrier.await(10, TimeUnit.SECONDS);
            distInterceptorBarrier.await(10, TimeUnit.SECONDS);
         }
         // Check that the put command didn't fail
         Object result = future.get(10, TimeUnit.SECONDS);
         assertNull(result);
         log.tracef("Put operation is done");

         // Check the value on the remaining node
         assertEquals("v", cache0.get(key));

         // Prepare for the next iteration...
         cache1.start();
         TestingUtil.waitForNoRebalance(cache0, cache1, cache2);
      }
   }
}
