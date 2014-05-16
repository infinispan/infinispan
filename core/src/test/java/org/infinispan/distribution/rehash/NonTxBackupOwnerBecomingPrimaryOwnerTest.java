package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.*;

/**
 * Tests data loss during state transfer a backup owner of a key becomes the primary owner
 * modified key while a putIfAbsent() call is in progress.
 * See https://issues.jboss.org/browse/ISPN-3357
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxBackupOwnerBecomingPrimaryOwnerTest")
@CleanupAfterMethod
public class NonTxBackupOwnerBecomingPrimaryOwnerTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = BasicCacheContainer.DEFAULT_CACHE_NAME;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.clustering().hash().numSegments(1).consistentHashFactory(new CustomConsistentHashFactory());
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   public void testPrimaryOwnerLeavingDuringPut() throws Exception {
      doTest(false);
   }

   public void testPrimaryOwnerLeavingDuringPutIfAbsent() throws Exception {
      doTest(true);
   }

   private void doTest(final boolean conditional) throws Exception {
      final String key = "testkey";

      CheckPoint checkPoint = new CheckPoint();
      LocalTopologyManager ltm0 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      int preJoinTopologyId = ltm0.getCacheTopology(CACHE_NAME).getTopologyId();

      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      addBlockingLocalTopologyManager(manager(0), checkPoint, preJoinTopologyId);

      final AdvancedCache<Object, Object> cache1 = advancedCache(1);
      addBlockingLocalTopologyManager(manager(1), checkPoint, preJoinTopologyId);

      // Add a new member and block the rebalance before the final topology is installed
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);
      addBlockingLocalTopologyManager(manager(2), checkPoint, preJoinTopologyId);

      log.tracef("Starting the cache on the joiner");
      final AdvancedCache<Object,Object> cache2 = advancedCache(2);
      int duringJoinTopologyId = preJoinTopologyId + 1;

      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(0));
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(2));

      // Wait for the write CH to contain the joiner everywhere
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getRpcManager().getMembers().size() == 3 &&
                  cache1.getRpcManager().getMembers().size() == 3 &&
                  cache2.getRpcManager().getMembers().size() == 3;
         }
      });

      CacheTopology duringJoinTopology = ltm0.getCacheTopology(CACHE_NAME);
      assertEquals(duringJoinTopologyId, duringJoinTopology.getTopologyId());
      assertNotNull(duringJoinTopology.getPendingCH());
      log.tracef("Rebalance started. Found key %s with current owners %s and pending owners %s", key,
            duringJoinTopology.getCurrentCH().locateOwners(key), duringJoinTopology.getPendingCH().locateOwners(key));

      // Every PutKeyValueCommand will be blocked before reaching the distribution interceptor on cache1
      CyclicBarrier beforeCache1Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor1 = new BlockingInterceptor(beforeCache1Barrier,
            PutKeyValueCommand.class, false);
      cache1.addInterceptorBefore(blockingInterceptor1, NonTxDistributionInterceptor.class);

      // Every PutKeyValueCommand will be blocked after returning to the distribution interceptor on cache2
      CyclicBarrier afterCache2Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor2 = new BlockingInterceptor(afterCache2Barrier,
            PutKeyValueCommand.class, true);
      cache2.addInterceptorBefore(blockingInterceptor2, StateTransferInterceptor.class);

      // Put from cache0 with cache0 as primary owner, cache2 will become the primary owner for the retry
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return conditional ? cache0.putIfAbsent(key, "v") : cache0.put(key, "v");
         }
      });

      // Wait for the command to be executed on cache2 and unblock it
      afterCache2Barrier.await(10, TimeUnit.SECONDS);
      afterCache2Barrier.await(10, TimeUnit.SECONDS);

      // Allow the topology update to proceed on all the caches
      int postJoinTopologyId = duringJoinTopologyId + 1;
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(0));
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(2));

      // Wait for the topology to change everywhere
      TestingUtil.waitForRehashToComplete(cache0, cache1, cache2);

      // Allow the put command to throw an OutdatedTopologyException on cache1
      log.tracef("Unblocking the put command on node " + address(1));
      beforeCache1Barrier.await(10, TimeUnit.SECONDS);
      beforeCache1Barrier.await(10, TimeUnit.SECONDS);

      // Allow the retry to proceed on cache1, if it's still a member.
      // (In my tests, the backup was always cache0.)
      CacheTopology postJoinTopology = ltm0.getCacheTopology(CACHE_NAME);
      if (postJoinTopology.getCurrentCH().locateOwners(key).contains(address(1))) {
         beforeCache1Barrier.await(10, TimeUnit.SECONDS);
         beforeCache1Barrier.await(10, TimeUnit.SECONDS);
      }
      // And allow the retry to finish successfully on cache2
      afterCache2Barrier.await(10, TimeUnit.SECONDS);
      afterCache2Barrier.await(10, TimeUnit.SECONDS);

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      if (conditional) {
         assertNull(result);
      } else {
         assertTrue(result == null || "v".equals(result));
      }
      log.tracef("Put operation is done");

      // Check the value on all the nodes
      assertEquals("v", cache0.get(key));
      assertEquals("v", cache1.get(key));
      assertEquals("v", cache2.get(key));

      // Check that there are no leaked locks
      assertFalse(cache0.getAdvancedCache().getLockManager().isLocked(key));
      assertFalse(cache1.getAdvancedCache().getLockManager().isLocked(key));
      assertFalse(cache2.getAdvancedCache().getLockManager().isLocked(key));
   }

   private static class CustomConsistentHashFactory extends BaseControlledConsistentHashFactory {
      private CustomConsistentHashFactory() {
         super(1);
      }

      @Override
      protected List<Address> createOwnersCollection(List<Address> members, int numberOfOwners, int segmentIndex) {
         assertEquals(2, numberOfOwners);
         if (members.size() == 1)
            return Arrays.asList(members.get(0));
         else if (members.size() == 2)
            return Arrays.asList(members.get(0), members.get(1));
         else
            return Arrays.asList(members.get(members.size() - 1), members.get(0));
      }
   }

   private void addBlockingLocalTopologyManager(final EmbeddedCacheManager manager, final CheckPoint checkPoint,
                                                final int currentTopologyId)
         throws InterruptedException {
      LocalTopologyManager component = TestingUtil.extractGlobalComponent(manager, LocalTopologyManager.class);
      LocalTopologyManager spyLtm = Mockito.spy(component);
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
            // Ignore the first topology update on the joiner, which is with the topology before the join
            if (topology.getTopologyId() != currentTopologyId) {
               checkPoint.trigger("pre_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress());
               checkPoint.await("allow_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress(),
                     10, TimeUnit.SECONDS);
            }
            return invocation.callRealMethod();
         }
      }).when(spyLtm).handleConsistentHashUpdate(eq(CacheContainer.DEFAULT_CACHE_NAME), any(CacheTopology.class),
            anyInt());
      TestingUtil.extractGlobalComponentRegistry(manager).registerComponent(spyLtm, LocalTopologyManager.class);
   }
}