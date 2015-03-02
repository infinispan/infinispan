package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
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
 * Tests that a conditional write is retried properly if the write is unsuccessful on the primary owner
 * because it became a non-owner and doesn't have the entry any more.
 * 
 * See https://issues.jboss.org/browse/ISPN-3830
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxPrimaryOwnerBecomingNonOwnerTest")
@CleanupAfterMethod
public class NonTxPrimaryOwnerBecomingNonOwnerTest extends MultipleCacheManagersTest {

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

   public void testPrimaryOwnerChangingDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE);
   }

   public void testPrimaryOwnerChangingDuringPutIfAbsent() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT);
   }

   public void testPrimaryOwnerChangingDuringReplace() throws Exception {
      doTest(TestWriteOperation.REPLACE);
   }

   public void testPrimaryOwnerChangingDuringReplaceExact() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT);
   }

   public void testPrimaryOwnerChangingDuringRemove() throws Exception {
      doTest(TestWriteOperation.REMOVE);
   }

   public void testPrimaryOwnerChangingDuringRemoveExact() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT);
   }

   private void doTest(final TestWriteOperation op) throws Exception {
      final String key = "testkey";
      if (op.getPreviousValue() != null) {
         cache(0, CACHE_NAME).put(key, op.getPreviousValue());
      }

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
      assertFalse(duringJoinTopology.getReadConsistentHash().equals(duringJoinTopology.getWriteConsistentHash()));

      log.tracef("Rebalance started. Found key %s with read CH owners %s and write CH owners %s", key,
                 duringJoinTopology.getReadConsistentHash().locateOwners(key), duringJoinTopology.getWriteConsistentHash().locateOwners(key));

      // Every operation command will be blocked before reaching the distribution interceptor on cache0 (the originator)
      CyclicBarrier beforeCache0Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor0 = new BlockingInterceptor(beforeCache0Barrier,
            op.getCommandClass(), false, true);
      cache0.addInterceptorBefore(blockingInterceptor0, EntryWrappingInterceptor.class);

      // Write from cache0 with cache0 as primary owner, cache2 will become the primary owner for the retry
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return op.perform(cache0, key);
         }
      });

      // Block the write command on cache0
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);

      // Allow the topology update to proceed on cache0
      final int postJoinTopologyId = duringJoinTopologyId + 1;
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(0));
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            CacheTopology cacheTopology = cache0.getComponentRegistry().getStateTransferManager().getCacheTopology();
            return cacheTopology.getTopologyId() == postJoinTopologyId;
         }
      });

      // Allow the command to proceed
      log.tracef("Unblocking the write command on node " + address(1));
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);

      // Wait for the retry after the OutdatedTopologyException
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);
      // And allow it to proceed
      beforeCache0Barrier.await(10, TimeUnit.SECONDS);

      // Allow the topology update to proceed on the other caches
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(2));

      // Wait for the topology to change everywhere
      TestingUtil.waitForRehashToComplete(cache0, cache1, cache2);

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("Write operation is done");

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));
      assertEquals(op.getValue(), cache2.get(key));

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
      Answer<Object> answer = new Answer<Object>() {
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
      };
      doAnswer(answer).when(spyLtm).handleRebalance(eq(CacheContainer.DEFAULT_CACHE_NAME), any(CacheTopology.class),
                                                    any(ConsistentHash.class), anyInt(), any(Address.class));
      doAnswer(answer).when(spyLtm).handleReadCHUpdate(eq(CacheContainer.DEFAULT_CACHE_NAME), any(CacheTopology.class),
                                                       any(AvailabilityMode.class), anyInt(), any(Address.class));
      doAnswer(answer).when(spyLtm).handleWriteCHUpdate(eq(CacheContainer.DEFAULT_CACHE_NAME), any(CacheTopology.class),
                                                        any(AvailabilityMode.class), anyInt(), any(Address.class));
      TestingUtil.extractGlobalComponentRegistry(manager).registerComponent(spyLtm, LocalTopologyManager.class);
   }
}
