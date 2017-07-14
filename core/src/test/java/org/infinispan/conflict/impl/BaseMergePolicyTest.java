package org.infinispan.conflict.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public abstract class BaseMergePolicyTest extends BasePartitionHandlingTest {

   private static Log log = LogFactory.getLog(BaseMergePolicyTest.class);
   private static boolean trace = log.isTraceEnabled();

   BaseMergePolicyTest() {
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   abstract void beforeSplit();
   abstract void duringSplit();
   abstract void afterMerge();

   public void testPartitionMergePolicy() throws Throwable {
      if (trace) log.tracef("beforeSplit()");
      beforeSplit();

      if (trace) log.tracef("splitCluster");
      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      TestingUtil.waitForNoRebalance(cache(0), cache(1));
      TestingUtil.waitForNoRebalance(cache(2), cache(3));

      if (trace) log.tracef("duringSplit()");
      duringSplit();

      if (trace) log.tracef("performMerge()");
      performMerge();

      if (trace) log.tracef("afterMerge()");
      afterMerge();
   }

   protected void performMerge() {
      partition(0).merge(partition(1));
      assertTrue(clusterAndChFormed(0, 4));
      assertTrue(clusterAndChFormed(1, 4));
      assertTrue(clusterAndChFormed(2, 4));
      assertTrue(clusterAndChFormed(3, 4));
      TestingUtil.waitForNoRebalance(caches());
   }

   protected <A, B> AdvancedCache<A, B> getCacheFromNonPreferredPartition(AdvancedCache... caches) {
      AdvancedCache<A, B> preferredCache = getCacheFromPreferredPartition(caches);
      List<AdvancedCache> cacheList = new ArrayList<>(Arrays.asList(caches));
      cacheList.remove(preferredCache);
      return cacheList.get(0);
   }

   protected <A, B> AdvancedCache<A, B> getCacheFromPreferredPartition(AdvancedCache... caches) {
      List<CacheStatusResponse> statusResponses = Arrays.stream(caches)
            .map(this::getCacheStatus)
            .flatMap(Collection::stream)
            .sorted(PreferAvailabilityStrategy.RESPONSE_COMPARATOR)
            .collect(Collectors.toList());

      CacheTopology maxStableTopology = null;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology stableTopology = response.getStableTopology();
         if (stableTopology == null) continue;

         if (maxStableTopology == null || maxStableTopology.getMembers().size() < stableTopology.getMembers().size()) {
            maxStableTopology = stableTopology;
         }
      }

      int cacheIndex = -1;
      int count = -1;
      CacheTopology maxTopology = null;
      for (CacheStatusResponse response : statusResponses) {
         count++;
         CacheTopology stableTopology = response.getStableTopology();
         if (!Objects.equals(stableTopology, maxStableTopology)) continue;

         CacheTopology topology = response.getCacheTopology();
         if (topology == null) continue;

         if (maxTopology == null || maxTopology.getMembers().size() < topology.getMembers().size()) {
            maxTopology = topology;
            cacheIndex = count;
         }
      }
      if (trace) log.tracef("getCacheFromPreferredPartition: partition=%s", maxTopology != null ? maxTopology.getCurrentCH().getMembers() : null);
      return caches[cacheIndex];
   }

   private Collection<CacheStatusResponse> getCacheStatus(AdvancedCache cache) {
      LocalTopologyManager localTopologyManager = cache.getComponentRegistry().getComponent(LocalTopologyManager.class);
      int viewId = cache.getRpcManager().getTransport().getViewId();
      ManagerStatusResponse statusResponse = localTopologyManager.handleStatusRequest(viewId);
      return statusResponse.getCaches().values();
   }

   protected void assertCacheGet(Object key, Object value, int... caches) {
      for (int index : caches) {
         AdvancedCache cache = advancedCache(index);
         String message = String.format("Key=%s, Value=%s, Cache Index=%s, Topology=%s", key, value, index, cache.getDistributionManager().getCacheTopology());
         assertEquals(message, value, cache.get(key));
      }
   }

   protected boolean clusterAndChFormed(int cacheIndex, int memberCount) {
      return advancedCache(cacheIndex).getRpcManager().getTransport().getMembers().size() == memberCount &&
            advancedCache(cacheIndex).getDistributionManager().getWriteConsistentHash().getMembers().size() == memberCount;
   }

   protected ConflictManager conflictManager(int index) {
      return ConflictManagerFactory.get(advancedCache(index));
   }

   protected void assertSameVersionAndNoConflicts(int cacheIndex, int numberOfVersions, Object key, Object expectedValue) {
      ConflictManager cm = conflictManager(cacheIndex);
      assert !cm.isConflictResolutionInProgress();
      Map<Address, InternalCacheValue> versionMap = cm.getAllVersions(key);
      assertNotNull(versionMap);
      assertEquals("Versions: " + versionMap, numberOfVersions, versionMap.size());
      String message = String.format("Key=%s. VersionMap: %s", key, versionMap);
      for (InternalCacheValue icv : versionMap.values()) {
         if (expectedValue != null) {
            assertNotNull(message, icv);
            assertNotNull(message, icv.getValue());
         }
         assertEquals(message, expectedValue, icv.getValue());
      }
      assertEquals(0, cm.getConflicts().count());
   }
}
