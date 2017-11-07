package org.infinispan.conflict.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.AvailabilityMode;
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

   protected MagicKey conflictKey;
   protected Object valueAfterMerge;
   protected PartitionDescriptor p0;
   protected PartitionDescriptor p1;
   protected int numberOfOwners;
   protected String description;

   BaseMergePolicyTest() {
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
      this.numberOfOwners = 2;
      this.valueAfterMerge = "DURING SPLIT";
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), new String[]{null});
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), description);
   }

   protected BaseMergePolicyTest setValueAfterMerge(Object val) {
      valueAfterMerge = val;
      return this;
   }

   protected BaseMergePolicyTest setPartitions(String description, AvailabilityMode mode, int[] partition1, int[] partition2) {
      this.description = description;
      p0 = new PartitionDescriptor(mode, partition1);
      p1 = new PartitionDescriptor(mode, partition2);
      numMembersInCluster = p0.getNodes().length + p1.getNodes().length;
      return this;
   }

   protected BaseMergePolicyTest setPartitions(String description, int[] partition1, int[] partition2) {
      return setPartitions(description, null, partition1, partition2);
   }

   protected void beforeSplit() {
      conflictKey = new MagicKey(cache(p0.node(0)), cache(p1.node(0)));
      cache(p0.node(0)).put(conflictKey, "BEFORE SPLIT");
   }

   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) {
      preferredPartitionCache.put(conflictKey, "DURING SPLIT");
   }

   protected void splitCluster() {
      splitCluster(p0.getNodes(), p1.getNodes());
      TestingUtil.waitForNoRebalance(getPartitionCaches(p0));
      TestingUtil.waitForNoRebalance(getPartitionCaches(p1));
   }

   protected void performMerge() {
      partition(0).merge(partition(1));
   }

   protected void afterConflictResolutionAndMerge() {
      ConflictManager cm = conflictManager(0);
      assert !cm.isConflictResolutionInProgress();
      Map<Address, InternalCacheValue> versionMap = cm.getAllVersions(conflictKey);
      assertNotNull(versionMap);
      assertEquals("Versions: " + versionMap, numberOfOwners, versionMap.size());
      String message = String.format("Key=%s. VersionMap: %s", conflictKey, versionMap);
      for (InternalCacheValue icv : versionMap.values()) {
         if (valueAfterMerge != null) {
            assertNotNull(message, icv);
            assertNotNull(message, icv.getValue());
            assertEquals(message, valueAfterMerge, icv.getValue());
         } else {
            assertNull(message, icv);
         }
      }
      assertEquals(0, cm.getConflicts().count());
   }

   public void testPartitionMergePolicy() throws Throwable {
      if (trace) log.tracef("beforeSplit()");
      beforeSplit();

      if (trace) log.tracef("splitCluster");
      splitCluster();

      if (trace) log.tracef("duringSplit()");
      AdvancedCache preferredPartitionCache = getCacheFromPreferredPartition();
      duringSplit(preferredPartitionCache, getCacheFromNonPreferredPartition(preferredPartitionCache));

      if (trace) log.tracef("performMerge()");
      performMerge();

      if (trace) log.tracef("afterConflictResolutionAndMerge()");
      afterConflictResolutionAndMerge();
   }

   protected AdvancedCache[] getPartitionCaches(PartitionDescriptor descriptor) {
      int[] nodes = descriptor.getNodes();
      AdvancedCache[] caches = new AdvancedCache[nodes.length];
      for (int i = 0; i < nodes.length; i++)
         caches[i] = advancedCache(nodes[i]);
      return caches;
   }

   protected <A, B> AdvancedCache<A, B> getCacheFromNonPreferredPartition(AdvancedCache preferredCache) {
      for (Cache c : caches()) {
         AdvancedCache cache = (AdvancedCache) c;
         if (!cache.getDistributionManager().getWriteConsistentHash().equals(preferredCache.getDistributionManager().getWriteConsistentHash()))
            return cache;
      }
      return null;
   }

   protected <A, B> AdvancedCache<A, B> getCacheFromPreferredPartition() {
      List<AdvancedCache> caches = caches().stream().map(AdvancedCache.class::cast).collect(Collectors.toList());
      return getCacheFromPreferredPartition(caches.toArray(new AdvancedCache[caches.size()]));
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

   protected int[] cacheIndexes() {
      int[] indexes = new int[numMembersInCluster];
      int count = 0;
      for (int i : p0.getNodes())
         indexes[count++] = i;
      return indexes;
   }
}
