package org.infinispan.statetransfer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Test if state transfer happens properly on a cache with pessimistic transactions.
 * See https://issues.jboss.org/browse/ISPN-2408.
 *
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferPessimisticTest")
@CleanupAfterMethod
public class StateTransferPessimisticTest extends MultipleCacheManagersTest {

   public static final int NUM_KEYS = 100;
   public static final int CLUSTER_SIZE = 2;
   private ConfigurationBuilder dccc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dccc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      dccc.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC);
      dccc.clustering().hash().numOwners(1).l1().disable();
      dccc.locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createCluster(dccc, CLUSTER_SIZE);
      waitForClusterToForm();
   }

   public void testStateTransfer() throws Exception {
      CyclicBarrier barrier = new CyclicBarrier(2);
      blockDataContainerIteration(cache(0), barrier);

      Set<Object> keys = new HashSet<>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = getKeyForCache(0);
         if (!keys.add(key)) continue;

         // put a key to have some data in cache
         cache(0).put(key, key);
      }

      log.trace("State transfer happens here");
      // add a third node
      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      // Wait for the stale entries invalidation to block
      barrier.await(10, TimeUnit.SECONDS);

      log.trace("Checking the values from caches...");
      for (Object key : keys) {
         // Expect one copy of each entry on the old owner, cache 0
         assertEquals(1, checkKey(key, cache(0)));
      }

      // Unblock the stale entries invalidation
      barrier.await(10, TimeUnit.SECONDS);
      cache(0).getAdvancedCache().getAsyncInterceptorChain().removeInterceptor(BlockingInterceptor.class);

      for (Object key : keys) {
         // Check that the stale entries on the old nodes are eventually deleted
         eventuallyEquals(1, () -> checkKey(key, cache(0), cache(1), cache(2)));
      }
   }

   public int checkKey(Object key, Cache... caches) {
      log.tracef("Checking key: %s", key);
      int c = 0;
      // check them directly in data container
      for (Cache cache : caches) {
         InternalCacheEntry e = cache.getAdvancedCache().getDataContainer().get(key);
         if (e != null) {
            assertEquals(key, e.getValue());
            c++;
         }
      }

      // look at them also via cache API
      for (Cache cache : caches) {
         assertEquals(key, cache.get(key));
      }
      return c;
   }

   protected void blockDataContainerIteration(final Cache<?, ?> cache, final CyclicBarrier barrier) {
      InternalDataContainer dataContainer = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(dataContainer);
      InternalDataContainer mockContainer = mock(InternalDataContainer.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         barrier.await(10, TimeUnit.SECONDS);
         // Now wait until main thread lets us through
         barrier.await(10, TimeUnit.SECONDS);

         return forwardedAnswer.answer(invocation);
      }).when(mockContainer).removeSegments(any());
      TestingUtil.replaceComponent(cache, InternalDataContainer.class, mockContainer, true);
   }
}
