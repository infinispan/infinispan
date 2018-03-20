package org.infinispan.partitionhandling.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.infinispan.partitionhandling.AvailabilityMode.AVAILABLE;
import static org.infinispan.test.TestingUtil.mapOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.util.logging.events.impl.EventLogManagerImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test how PreferAvailabilityStrategy picks the post-merge topology in different scenarios.
 *
 * @author Dan Berindei
 * @since 9.2
 */
@Test(groups = "unit", testName = "partitionhandling.impl.PreferAvailabilityStrategyNoConflictResolutionTest")
public class PreferAvailabilityStrategyTest extends AbstractInfinispanTest {
   private static final CacheJoinInfo JOIN_INFO =
      new CacheJoinInfo(new DefaultConsistentHashFactory(), MurmurHash3.getInstance(), 8, 2, 1000, false,
                        CacheMode.DIST_SYNC, 1.0f, null, Optional.empty());
   private static final Address A = new TestAddress(1, "A");
   private static final Address B = new TestAddress(2, "B");
   private static final Address C = new TestAddress(3, "C");
   public static final String CACHE_NAME = "test";

   private EventLogManagerImpl eventLogManager;
   private PersistentUUIDManagerImpl persistentUUIDManager;
   private AvailabilityStrategyContext context;
   private PreferAvailabilityStrategy strategy;

   @DataProvider(name = "dp")
   public Object[][] data() {
      return new Object[][] {{true},{false}};
   }

   @BeforeMethod(alwaysRun = true)
   public void setup() {
      persistentUUIDManager = new PersistentUUIDManagerImpl();
      eventLogManager = new EventLogManagerImpl();
      context = mock(AvailabilityStrategyContext.class);

      persistentUUIDManager.addPersistentAddressMapping(A, TestClusterCacheStatus.persistentUUID(A));
      persistentUUIDManager.addPersistentAddressMapping(B, TestClusterCacheStatus.persistentUUID(B));
      persistentUUIDManager.addPersistentAddressMapping(C, TestClusterCacheStatus.persistentUUID(C));

      strategy = new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager,
                                                ClusterTopologyManagerImpl::distLostDataCheck);
   }

   public void testCoordinatorChangeTopologyNotUpdatedAfterLeave() {
      TestClusterCacheStatus cache = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      List<Address> remainingMembers = asList(A, B);
      CacheStatusResponse responseA = availableResponse(A, cache);
      CacheStatusResponse responseB = availableResponse(B, cache);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(true);

      strategy.onPartitionMerge(context, statusResponses);

      // FIXME Either the stable topology id should also be incremented, or even better the current topology id
      // shouldn't change if all members report the same topology
      cache.incrementIds(1, 1);
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      verify(context).updateTopologiesAfterMerge(cache.topology(), cache.stableTopology(), null, false);
      verify(context).updateCurrentTopology(remainingMembers);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   @Test(dataProvider = "dp")
   public void testCoordinatorChangeTopologyPartiallyUpdatedAfterLeave(boolean resolveConflicts) {
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      List<Address> remainingMembers = asList(A, B);
      TestClusterCacheStatus cacheStatusB = cacheA.copy();
      cacheStatusB.removeMember(C);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheStatusB);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(resolveConflicts);

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      if (resolveConflicts) {
         cacheA.incrementIds(1, 0);
         cacheA.startConflictResolution(A, B);
         verify(context).calculateConflictHash(distinctHashes(statusResponses));
      } else {
         cacheA.incrementIds(2, 1);
      }
      // FIXME Node B's topology is more recent, so it should be used instead
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, resolveConflicts);
      if (!resolveConflicts) verify(context).updateCurrentTopology(remainingMembers);
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   @Test(dataProvider = "dp")
   public void testCoordinatorChangeLeaveDuringRebalanceReadOld(boolean resolveConflicts) {
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B);
      List<Address> remainingMembers = asList(A, C);
      TestClusterCacheStatus cacheC = cacheA.copy();
      cacheC.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheC.removeMember(B);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(resolveConflicts);
      when(context.getStableTopology()).thenReturn(cacheA.stableTopology());
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      if (resolveConflicts) {
         cacheA.incrementIds(2, 1);
         cacheA.startConflictResolution(A,C);
         verify(context).calculateConflictHash(distinctHashes(statusResponses));
      } else {
         cacheA.incrementIds(3, 2);
      }
      // FIXME Node C's topology is more recent, so it should be used instead
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, resolveConflicts);
      // FIXME Shouldn't be used at all during merge
      verify(context).getStableTopology();
      verify(context).getCacheName();
      if (!resolveConflicts)  verify(context).updateCurrentTopology(singletonList(A));
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   @Test(dataProvider = "dp")
   public void testCoordinatorChangeLeaveDuringRebalanceReadNew(boolean resolveConflicts) {
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B);
      cacheA.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheA.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      List<Address> remainingMembers = asList(A, C);
      TestClusterCacheStatus cacheC = cacheA.copy();
      cacheC.removeMember(B);
      cacheC.advanceRebalance(CacheTopology.Phase.READ_NEW_WRITE_ALL);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, C, responseC);

      when(context.getExpectedMembers()).thenReturn(remainingMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(resolveConflicts);
      when(context.getStableTopology()).thenReturn(cacheA.stableTopology());
      when(context.getCacheName()).thenReturn(CACHE_NAME);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.cancelRebalance();
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      if (resolveConflicts) {
         cacheA.incrementIds(1, 0);
         cacheA.startConflictResolution(A, C);
         verify(context).calculateConflictHash(distinctHashes(statusResponses));
      } else {
         cacheA.incrementIds(2, 1);
      }
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, resolveConflicts);
      // FIXME Shouldn't be used here
      verify(context).getStableTopology();
      verify(context).getCacheName();
      if (!resolveConflicts) verify(context).updateCurrentTopology(singletonList(A));
      verify(context).queueRebalance(remainingMembers);
      verifyNoMoreInteractions(context);
   }

   @Test(dataProvider = "dp")
   public void testMergeWithPausedNodeDuringRebalance(boolean resolveConflicts) {
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      TestClusterCacheStatus cacheC = cacheB.copy();
      cacheC.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      CacheStatusResponse responseC = availableResponse(C, cacheC);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(resolveConflicts);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.cancelRebalance();
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      if (resolveConflicts) {
         verify(context).calculateConflictHash(distinctHashes(statusResponses));
         cacheA.incrementIds(1, 1);
         cacheA.startConflictResolution(A, B, C);
      } else {
         cacheA.incrementIds(2, 2);
      }

      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, resolveConflicts);
      if (!resolveConflicts) verify(context).updateCurrentTopology(asList(A, B, C));
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   @Test(dataProvider = "dp")
   public void testMergeWithPausedNodeAfterRebalance(boolean resolveConflicts) {
      List<Address> mergeMembers = asList(A, B, C);
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B, C);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.startRebalance(CacheTopology.Phase.READ_OLD_WRITE_ALL, A, B, C);
      cacheB.advanceRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL);
      cacheB.finishRebalance();
      cacheB.updateStableTopology();
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      CacheStatusResponse responseC = availableResponse(C, cacheB);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB, C, responseC);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(resolveConflicts);

      strategy.onPartitionMerge(context, statusResponses);

      cacheA.cancelRebalance();
      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      if (resolveConflicts) {
         verify(context).calculateConflictHash(distinctHashes(statusResponses));
         cacheA.incrementIds(2, 1);
         cacheA.startConflictResolution(A, B, C);
      } else {
         cacheA.incrementIds(3, 2);
      }
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, resolveConflicts);
      if (!resolveConflicts) verify(context).updateCurrentTopology(asList(A, B, C));
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   @Test(dataProvider = "dp", description = "ISPN-8903")
   public void testMergeWithPausedNodeTriggersCR(boolean resolveConflicts) {
      List<Address> mergeMembers = asList(A, B);
      TestClusterCacheStatus cacheA = TestClusterCacheStatus.start(JOIN_INFO, A, B);
      TestClusterCacheStatus cacheB = cacheA.copy();
      cacheB.removeMember(A);
      CacheStatusResponse responseA = availableResponse(A, cacheA);
      CacheStatusResponse responseB = availableResponse(B, cacheB);
      Map<Address, CacheStatusResponse> statusResponses = mapOf(A, responseA, B, responseB);

      when(context.getExpectedMembers()).thenReturn(mergeMembers);
      when(context.resolveConflictsOnMerge()).thenReturn(resolveConflicts);

      strategy.onPartitionMerge(context, statusResponses);

      verify(context).getExpectedMembers();
      verify(context).resolveConflictsOnMerge();
      if (resolveConflicts) {
         verify(context).calculateConflictHash(distinctHashes(statusResponses));
         cacheA.incrementIds(1, 0);
         cacheA.startConflictResolution(A, B);
      } else {
         cacheA.incrementIds(2, 1);
      }
      // FIXME This is wrong, should use the topologies of B and C
      verify(context).updateTopologiesAfterMerge(cacheA.topology(), cacheA.stableTopology(), null, resolveConflicts);
      if (!resolveConflicts) verify(context).updateCurrentTopology(asList(A, B));
      verify(context).queueRebalance(mergeMembers);
      verifyNoMoreInteractions(context);
   }

   private CacheStatusResponse availableResponse(Address a, TestClusterCacheStatus cacheStatus) {
      return new CacheStatusResponse(cacheStatus.joinInfo(a), cacheStatus.topology(), cacheStatus.stableTopology(),
                                     AVAILABLE);
   }

   private Set<ConsistentHash> distinctHashes(Map<Address, CacheStatusResponse> statusResponses) {
      return statusResponses.values().stream()
            .map(CacheStatusResponse::getCacheTopology)
            .filter(Objects::nonNull)
            .map(CacheTopology::getCurrentCH)
            .filter(h -> h != null && !h.getMembers().isEmpty())
            .collect(Collectors.toSet());
   }
}
