package org.infinispan.conflict.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.MagicKey;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.LostDataCheck;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterTopologyManagerImpl;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public abstract class BaseMergePolicyTest extends BasePartitionHandlingTest {

   private static final Log log = LogFactory.getLog(BaseMergePolicyTest.class);

   protected MagicKey conflictKey;
   protected Object valueAfterMerge;
   protected PartitionDescriptor p0;
   protected PartitionDescriptor p1;
   protected String description;

   protected BaseMergePolicyTest() {
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
      this.valueAfterMerge = "DURING SPLIT";
   }

   protected BaseMergePolicyTest(CacheMode cacheMode, String description, int[] partition1, int[] partition2) {
      this(cacheMode, 2, description, null, partition1, partition2);
   }

   protected BaseMergePolicyTest(CacheMode cacheMode, String description, AvailabilityMode availabilityMode,
                       int[] partition1, int[] partition2) {
      this(cacheMode, 2, description, availabilityMode, partition1, partition2);
   }

   protected BaseMergePolicyTest(CacheMode cacheMode, int numOwners, String description, AvailabilityMode availabilityMode,
                                 int[] partition1, int[] partition2) {
      this();
      this.cacheMode = cacheMode;
      this.description = description;
      p0 = new PartitionDescriptor(availabilityMode, partition1);
      p1 = new PartitionDescriptor(availabilityMode, partition2);
      numMembersInCluster = p0.getNodes().length + p1.getNodes().length;

      if (cacheMode == CacheMode.REPL_SYNC) {
         numberOfOwners = numMembersInCluster;
      } else {
         this.numberOfOwners = numOwners;
      }
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), new String[]{null});
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), description);
   }


   protected void beforeSplit() {
      conflictKey = numberOfOwners > 1 ? new MagicKey(cache(p0.node(0)), cache(p1.node(0))) : new MagicKey(cache(p0.node(0)));
      cache(p0.node(0)).put(conflictKey, "BEFORE SPLIT");
   }

   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache)
         throws Exception {
      preferredPartitionCache.put(conflictKey, "DURING SPLIT");
   }

   protected void splitCluster() {
      splitCluster(p0.getNodes(), p1.getNodes());
      TestingUtil.waitForNoRebalance(getPartitionCaches(p0));
      TestingUtil.waitForNoRebalance(getPartitionCaches(p1));
   }

   protected void performMerge() throws Exception {
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

   public void testPartitionMergePolicy() throws Exception {
      log.tracef("beforeSplit()");
      beforeSplit();

      log.tracef("splitCluster");
      splitCluster();

      log.tracef("duringSplit()");
      AdvancedCache preferredPartitionCache = getCacheFromPreferredPartition();
      duringSplit(preferredPartitionCache, getCacheFromNonPreferredPartition(preferredPartitionCache));

      log.tracef("performMerge()");
      performMerge();

      log.tracef("afterConflictResolutionAndMerge()");
      afterConflictResolutionAndMerge();
   }

   protected <K, V> AdvancedCache<K, V> getCacheFromNonPreferredPartition(AdvancedCache preferredCache) {
      for (Cache c : caches()) {
         AdvancedCache cache = (AdvancedCache) c;
         if (!cache.getDistributionManager().getWriteConsistentHash().equals(preferredCache.getDistributionManager().getWriteConsistentHash()))
            return cache;
      }
      return null;
   }

   protected <K, V> AdvancedCache<K, V> getCacheFromPreferredPartition() {
      AdvancedCache[] caches = caches().stream().map(Cache::getAdvancedCache).toArray(AdvancedCache[]::new);
      return getCacheFromPreferredPartition(caches);
   }

   protected <K, V> AdvancedCache<K, V> getCacheFromPreferredPartition(AdvancedCache... caches) {
      Map<Address, CacheStatusResponse> statusResponses =
         Arrays.stream(caches).collect(Collectors.toMap(this::address, this::getCacheStatus));

      LostDataCheck lostDataCheck = ClusterTopologyManagerImpl::distLostDataCheck;
      CacheTopology preferredTopology = new PreferAvailabilityStrategy(null, null, lostDataCheck)
                                           .computePreferredTopology(statusResponses);

      log.tracef("getCacheFromPreferredPartition: partition=%s", preferredTopology.getMembers());
      return Arrays.stream(caches)
                   .filter(c -> address(c).equals(preferredTopology.getMembers().get(0)))
                   .findFirst().get();
   }

   private CacheStatusResponse getCacheStatus(AdvancedCache cache) {
      LocalTopologyManager localTopologyManager = ComponentRegistry.componentOf(cache, LocalTopologyManager.class);
      int viewId = cache.getRpcManager().getTransport().getViewId();
      ManagerStatusResponse statusResponse = CompletionStages.join(localTopologyManager.handleStatusRequest(viewId));
      return statusResponse.caches().get(cache.getName());
   }

   protected void assertCacheGet(Object key, Object value, int... caches) {
      for (int index : caches) {
         AdvancedCache cache = advancedCache(index);
         String message = String.format("Key=%s, Value=%s, Cache Index=%s, Topology=%s", key, value, index, cache.getDistributionManager().getCacheTopology());
         assertEquals(message, value, cache.get(key));
      }
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
