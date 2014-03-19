package org.infinispan.statetransfer;

import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.ClusterTopologyManager;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Test if state transfer happens properly on a non-tx invalidation cache.
 *
 * @since 7.0
 */
@Test(groups = "functional", testName = "statetransfer.NonTxStateTransferInvalidationTest")
@CleanupAfterMethod
public class NonTxStateTransferInvalidationTest extends MultipleCacheManagersTest {

   public static final int NUM_KEYS = 100;
   private ConfigurationBuilder dccc;

   @Override
   protected void createCacheManagers() throws Throwable {
      dccc = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, false, true);
      createCluster(dccc, 2);
      waitForClusterToForm();
   }

   public void testStateTransfer() throws Exception {
      // Insert initial data in the cache
      Set<Object> keys = new HashSet<Object>();
      for (int i = 0; i < NUM_KEYS; i++) {
         Object key = "key" + i;
         keys.add(key);
         cache(0).put(key, key);
      }

      log.trace("State transfer happens here");
      // add a third node
      addClusterEnabledCacheManager(dccc);
      waitForClusterToForm();

      log.trace("Checking the values from caches...");
      int keysOnJoiner = 0;
      for (Object key : keys) {
         log.tracef("Checking key: %s", key);
         // check them directly in data container
         InternalCacheEntry d0 = advancedCache(0).getDataContainer().get(key);
         InternalCacheEntry d1 = advancedCache(1).getDataContainer().get(key);
         InternalCacheEntry d2 = advancedCache(2).getDataContainer().get(key);
         assertEquals(key, d0.getValue());
         assertNull(d1);
         if (d2 != null) {
            keysOnJoiner++;
         }
      }

      assertTrue("The joiner should receive at least one key", keysOnJoiner > 0);
   }

   @Test(groups = "unstable", description = "See ISPN-4016")
   public void testInvalidationDuringStateTransfer() throws Exception {
      cache(0).put("key1", "value1");

      CheckPoint checkPoint = new CheckPoint();
      blockJoinResponse(manager(0), checkPoint);

      addClusterEnabledCacheManager(dccc);
      Future<Object> joinFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            // The cache only joins here
            return cache(2);
         }
      });

      checkPoint.awaitStrict("sending_join_response", 10, SECONDS);

      // This will invoke an invalidation on the joiner
      NotifyingFuture<Object> putFuture = cache(0).putAsync("key2", "value2");
      try {
         putFuture.get(1, SECONDS);
         fail("Put operation should have been blocked, but it finished successfully");
      } catch (java.util.concurrent.TimeoutException e) {
         // expected
      }

      checkPoint.trigger("resume_join_response");
      putFuture.get(10, SECONDS);
   }

   private void blockJoinResponse(final EmbeddedCacheManager manager, final CheckPoint checkPoint)
         throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager, ClusterTopologyManager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(ctm);
      ClusterTopologyManager mockManager = mock(ClusterTopologyManager.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object answer = forwardedAnswer.answer(invocation);
            checkPoint.trigger("sending_join_response");
            checkPoint.awaitStrict("resume_join_response", 10, SECONDS);
            return answer;
         }
      }).when(mockManager).handleJoin(anyString(), any(Address.class), any(CacheJoinInfo.class), anyInt());
      TestingUtil.replaceComponent(manager, ClusterTopologyManager.class, mockManager, true);
   }

}
