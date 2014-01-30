package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.L1Manager;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.SingleSegmentConsistentHashFactory;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Test that ensure when L1 cache is enabled that if writes occurs during a state transfer and vice versa that the
 * proper data is available.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.L1StateTransferRemovesValueTest")
public class L1StateTransferRemovesValueTest extends BaseDistFunctionalTest<String, String> {
   public L1StateTransferRemovesValueTest() {
      INIT_CLUSTER_SIZE = 3;
      numOwners = 2;
      tx = false;
      performRehashing = true;
      l1CacheEnabled = true;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private final String key = this.getClass() + "-key";
   private final String startValue = "starting-value";
   private final String newValue = "new-value";

   protected final ControllableConsistentHashFactory factory = new ControllableConsistentHashFactory();

   @AfterMethod
   public void resetFactory() {
      factory.setMembersToUse(null);
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder.clustering().hash().
            consistentHashFactory(factory).
            numSegments(1);
      return builder;
   }

   @Test
   public void testStateTransferWithRequestorsForNonExistentL1Value() throws Exception {
      // First 2 caches are primary and backup respectively at the beginning
      L1Manager l1Manager = c1.getAdvancedCache().getComponentRegistry().getComponent(L1Manager.class);
      l1Manager.addRequestor(key, c3.getCacheManager().getAddress());

      assertNull(c3.get(key));

      // Block the rebalance confirmation on nonOwnerCache
      CheckPoint checkPoint = new CheckPoint();
      // We have to wait until non owner has the new topology installed before transferring state
      waitUntilToplogyInstalled(c3, checkPoint);
      // Now make sure the owners doesn't have the new topology installed
      waitUntilBeforeTopologyInstalled(c1, checkPoint);
      waitUntilBeforeTopologyInstalled(c2, checkPoint);

      // Now force 1 and 3 to be owners so then 3 will get invalidation and state transfer
      factory.setMembersToUse(new boolean[]{true, false, true, false});

      EmbeddedCacheManager cm = addClusterEnabledCacheManager(configuration);

      Future<Void> join = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitForClusterToForm(cacheName);
            log.debug("4th has joined");
            return null;
         }
      });

      checkPoint.awaitStrict("post_topology_installed_invoked_" + c3, 10, TimeUnit.SECONDS);
      checkPoint.awaitStrict("pre_topology_installed_invoked_" + c1, 10, TimeUnit.SECONDS);
      checkPoint.awaitStrict("pre_topology_installed_invoked_" + c2, 10, TimeUnit.SECONDS);

      assertNull(c1.put(key, newValue));

      checkPoint.triggerForever("post_topology_installed_released_" + c3);
      checkPoint.triggerForever("pre_topology_installed_released_" + c1);
      checkPoint.triggerForever("pre_topology_installed_released_" + c2);

      join.get(10, TimeUnit.SECONDS);

      assertIsInContainerImmortal(c1, key);
      assertIsNotInL1(c2, key);
      assertIsInContainerImmortal(c3, key);
      assertIsNotInL1(cm.getCache(cacheName), key);

      // Make sure the ownership is all good still
      assertTrue(DistributionTestHelper.isOwner(c1, key));
      assertFalse(DistributionTestHelper.isOwner(c2, key));
      assertTrue(DistributionTestHelper.isOwner(c3, key));
      assertFalse(DistributionTestHelper.isOwner(cm.getCache(cacheName), key));
   }

   @Test
   public void testStateTransferWithL1InvalidationAboutToBeCommitted() throws Exception {
      // First 2 caches are primary and backup respectively at the beginning
      c1.put(key, startValue);

      assertEquals(startValue, c3.get(key));

      assertIsInL1(c3, key);

      CyclicBarrier barrier = new CyclicBarrier(2);
      c3.getAdvancedCache().addInterceptorAfter(new BlockingInterceptor(barrier, InvalidateL1Command.class, true),
                                                EntryWrappingInterceptor.class);

      Future<String> future = c1.putAsync(key, newValue);

      barrier.await(10, TimeUnit.SECONDS);

      // Block the rebalance confirmation on nonOwnerCache
      CheckPoint checkPoint = new CheckPoint();
      // We have to wait until non owner has the new topology installed before transferring state
      waitUntilToplogyInstalled(c3, checkPoint);
      // Now make sure the owners doesn't have the new topology installed
      waitUntilBeforeTopologyInstalled(c1, checkPoint);
      waitUntilBeforeTopologyInstalled(c2, checkPoint);

      // Now force 1 and 3 to be owners so then 3 will get invalidation and state transfer
      factory.setMembersToUse(new boolean[]{true, false, true, false});

      EmbeddedCacheManager cm = addClusterEnabledCacheManager(configuration);

      Future<Void> join = fork(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            waitForClusterToForm(cacheName);
            log.debug("4th has joined");
            return null;
         }
      });

      checkPoint.awaitStrict("post_topology_installed_invoked_" + c3, 10, TimeUnit.SECONDS);
      checkPoint.awaitStrict("pre_topology_installed_invoked_" + c1, 10, TimeUnit.SECONDS);
      checkPoint.awaitStrict("pre_topology_installed_invoked_" + c2, 10, TimeUnit.SECONDS);

      barrier.await(10, TimeUnit.SECONDS);
      assertEquals(startValue, future.get(10, TimeUnit.SECONDS));

      checkPoint.triggerForever("post_topology_installed_released_" + c3);
      checkPoint.triggerForever("pre_topology_installed_released_" + c1);
      checkPoint.triggerForever("pre_topology_installed_released_" + c2);

      join.get(10, TimeUnit.SECONDS);

      assertIsInContainerImmortal(c1, key);
      assertIsNotInL1(c2, key);
      assertIsInContainerImmortal(c3, key);
      assertIsNotInL1(cm.getCache(cacheName), key);

      // Make sure the ownership is all good still
      assertTrue(DistributionTestHelper.isOwner(c1, key));
      assertFalse(DistributionTestHelper.isOwner(c2, key));
      assertTrue(DistributionTestHelper.isOwner(c3, key));
      assertFalse(DistributionTestHelper.isOwner(cm.getCache(cacheName), key));
   }

   protected void waitUntilBeforeTopologyInstalled(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateConsumer sc = TestingUtil.extractComponent(cache, StateConsumer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sc);
      StateConsumer mockConsumer = mock(StateConsumer.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            // Wait for main thread to sync up
            checkPoint.trigger("pre_topology_installed_invoked_" + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("pre_topology_installed_released_" + cache, 10, TimeUnit.SECONDS);

            return forwardedAnswer.answer(invocation);
         }
      }).when(mockConsumer).onTopologyUpdate(any(CacheTopology.class), anyBoolean());
      TestingUtil.replaceComponent(cache, StateConsumer.class, mockConsumer, true);
   }

   protected void waitUntilToplogyInstalled(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateTransferLock sc = TestingUtil.extractComponent(cache, StateTransferLock.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sc);
      StateTransferLock mockConsumer = mock(StateTransferLock.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object answer = forwardedAnswer.answer(invocation);
            // Wait for main thread to sync up
            checkPoint.trigger("post_topology_installed_invoked_" + cache);
            // Now wait until main thread lets us through
            checkPoint.awaitStrict("post_topology_installed_released_" + cache, 10, TimeUnit.SECONDS);
            return answer;
         }
      }).when(mockConsumer).notifyTopologyInstalled(anyInt());
      TestingUtil.replaceComponent(cache, StateTransferLock.class, mockConsumer, true);
   }

   public static class ControllableConsistentHashFactory extends SingleSegmentConsistentHashFactory {

      private boolean[] membersToUse;

      public void setMembersToUse(boolean[] shouldUseMember) {
         membersToUse = shouldUseMember;
      }

      @Override
      protected final List<Address> createOwnersCollection(List<Address> members, int numberOfOwners) {
         // The owners will be the first <i>numOwners</i> in <i>member</i> unless {@link #membersToUse} is set, then
         // the first <i>numOwners</i> of <i>members</i> that are contained in {@link #membersToUse}
         List<Address> owners = new ArrayList<Address>(numberOfOwners);
         for (int i = 0; i < members.size() && numberOfOwners > owners.size(); ++i) {
            if (membersToUse == null || membersToUse.length > i && membersToUse[i]) {
               owners.add(members.get(i));
            }
         }
         return owners;
      }
   }
}
