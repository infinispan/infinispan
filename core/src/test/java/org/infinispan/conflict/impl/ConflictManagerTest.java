package org.infinispan.conflict.impl;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.ConflictManagerFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "conflict.resolution.ConflictManagerTest")
public class ConflictManagerTest extends BasePartitionHandlingTest {

   private static final String CACHE_NAME = "conflict-cache";
   private static final int NUMBER_OF_OWNERS = 2;
   private static final int NUMBER_OF_CACHE_ENTRIES = 100;
   private static final int INCONSISTENT_VALUE_INCREMENT = 10;
   private static final int NULL_VALUE_FREQUENCY = 20;
   private static final int STATE_TRANSFER_DELAY = 4000;

   public ConflictManagerTest() {
      this.cacheMode = CacheMode.DIST_SYNC;
      this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(partitionHandling).mergePolicy(null).stateTransfer().fetchInMemoryState(true);
      defineConfigurationOnAllManagers(CACHE_NAME, builder);
   }

   public void testGetAllVersionsDuringStateTransfer() throws Exception {
      final int key = 1;
      final int value = 1;
      createCluster();
      getCache(2).put(key, value);
      splitCluster();

      delayStateTransferCompletion();
      RehashListener listener = new RehashListener();
      getCache(2).addListener(listener);

      Future<Map<Address, InternalCacheValue<Object>>> versionFuture = fork(() -> {
         while (!listener.isRehashInProgress.get())
            TestingUtil.sleepThread(100);

         return getAllVersions(0, key);
      });
      fork(() -> partition(0).merge(partition(1)));

      Map<Address, InternalCacheValue<Object>> versionMap = versionFuture.get(STATE_TRANSFER_DELAY * 8, TimeUnit.MILLISECONDS);
      assertTrue(versionMap != null);
      assertTrue(!versionMap.isEmpty());
      assertEquals(String.format("Returned versionMap %s", versionMap),2, versionMap.size());
      versionMap.values().forEach(icv -> assertEquals(value, icv.getValue()));
   }

   @Test(expectedExceptions = CacheException.class,
         expectedExceptionsMessageRegExp = ".* encountered when attempting '.*.' on cache '.*.'")
   public void testGetAllVersionsTimeout() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().remoteTimeout(5000).stateTransfer().fetchInMemoryState(true);
      String cacheName = CACHE_NAME + "2";
      defineConfigurationOnAllManagers(cacheName, builder);
      waitForClusterToForm(cacheName);
      dropClusteredGetCommands();
      getAllVersions(0, "Test");
   }

   @Test(expectedExceptions = IllegalStateException.class,
         expectedExceptionsMessageRegExp = ".* Unable to retrieve conflicts as StateTransfer is currently in progress for cache .*")
   public void testGetConflictsDuringStateTransfer() throws Throwable {
      createCluster();
      splitCluster();
      delayStateTransferCompletion();
      RehashListener listener = new RehashListener();
      getCache(2).addListener(listener);

      Future<Void> conflictsFuture = fork(() -> {
         while (!listener.isRehashInProgress.get())
            TestingUtil.sleepThread(100);

         getConflicts(0);
         return null;
      });
      Future<?> mergeFuture = fork(() -> partition(0).merge(partition(1)));
      try {
         conflictsFuture.get(60, TimeUnit.SECONDS); // Same timeout as used by TestingUtil::waitForNoRebalance
         mergeFuture.get(); // No timeout as partition::merge eventually throws a TimeoutException
      } catch (ExecutionException e) {
         throw e.getCause();
      }
   }

   public void testAllVersionsOfKeyReturned() {
      // Test with and without conflicts
      waitForClusterToForm(CACHE_NAME);
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> getCache(0).put(i, "v" + i));
      compareCacheValuesForKey(INCONSISTENT_VALUE_INCREMENT, true);
      introduceCacheConflicts();
      compareCacheValuesForKey(INCONSISTENT_VALUE_INCREMENT, false);
      compareCacheValuesForKey(NULL_VALUE_FREQUENCY, false);
   }

   public void testConsecutiveInvocationOfAllVersionsForKey() throws Exception {
      waitForClusterToForm(CACHE_NAME);
      int key = 1;
      Map<Address, InternalCacheValue<Object>> result1 = getAllVersions(0, key);
      Map<Address, InternalCacheValue<Object>> result2 = getAllVersions(0, key);
      assertNotSame(result1, result2); // Assert that a different map is returned, i.e. a new CompletableFuture was created
      assertEquals(result1, result2); // Assert that returned values are still logically equivalent
   }

   public void testConflictsDetected() {
      // Test that no conflicts are detected at the start
      // Deliberately introduce conflicts and make sure they are detected
      waitForClusterToForm(CACHE_NAME);
      IntStream.range(0, NUMBER_OF_CACHE_ENTRIES).forEach(i -> getCache(0).put(i, "v" + i));
      final int cacheIndex = numMembersInCluster - 1;
      assertEquals(0, getConflicts(cacheIndex).count());
      introduceCacheConflicts();
      List<Map<Address, CacheEntry<Object, Object>>> conflicts = getConflicts(cacheIndex).collect(Collectors.toList());

      assertEquals(INCONSISTENT_VALUE_INCREMENT, conflicts.size());
      for (Map<Address, CacheEntry<Object, Object>> map : conflicts) {
         assertEquals(NUMBER_OF_OWNERS, map.keySet().size());
         Collection<CacheEntry<Object, Object>> mapValues = map.values();
         int key = mapValues.stream().filter(e -> !(e instanceof NullCacheEntry)).mapToInt(e -> (Integer) e.getKey()).findAny().orElse(-1);
         assertTrue(key > -1);
         if (key % NULL_VALUE_FREQUENCY == 0) {
            assertTrue(map.values().stream().anyMatch(NullCacheEntry.class::isInstance));
         } else {
            List<Object> icvs = map.values().stream().map(CacheEntry::getValue).distinct().collect(Collectors.toList());
            assertEquals(NUMBER_OF_OWNERS, icvs.size());
            assertTrue("Expected one of the conflicting string values to be 'INCONSISTENT'", icvs.contains("INCONSISTENT"));
         }
      }
   }

   public void testConflictsResolvedWithProvidedMergePolicy() {
      createCluster();
      AdvancedCache<Object, Object> cache = getCache(0);
      ConflictManager<Object, Object> cm = ConflictManagerFactory.get(cache);
      MagicKey key = new MagicKey(cache(0), cache(1));
      cache.put(key, 1);
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, 2);
      assertEquals(1, getConflicts(0).count());
      cm.resolveConflicts(((preferredEntry, otherEntries) -> preferredEntry));
      assertEquals(0, getConflicts(0).count());
   }

   public void testCacheOperationOnConflictStream() {
      createCluster();
      AdvancedCache<Object, Object> cache = getCache(0);
      ConflictManager<Object, Object> cm = ConflictManagerFactory.get(cache);
      MagicKey key = new MagicKey(cache(0), cache(1));
      cache.put(key, 1);
      cache.withFlags(Flag.CACHE_MODE_LOCAL).put(key, 2);
      cm.getConflicts().forEach(map -> {
         CacheEntry<Object, Object> entry = map.values().iterator().next();
         Object conflictKey = entry.getKey();
         cache.remove(conflictKey);
      });
      assertTrue(cache.isEmpty());
   }

   @Test(expectedExceptions = CacheException.class)
   private void testNoEntryMergePolicyConfigured() {
      ConflictManagerFactory.get(getCache(0)).resolveConflicts();
   }

   private void introduceCacheConflicts() {
      LocalizedCacheTopology topology = getCache(0).getDistributionManager().getCacheTopology();
      for (int i = 0; i < NUMBER_OF_CACHE_ENTRIES; i += INCONSISTENT_VALUE_INCREMENT) {
         Address primary = topology.getDistribution(i).primary();
         AdvancedCache<Object, Object> primaryCache = manager(primary).getCache(CACHE_NAME).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);

         if (i % NULL_VALUE_FREQUENCY == 0)
            primaryCache.remove(i);
         else
            primaryCache.put(i, "INCONSISTENT");
      }
   }

   private void compareCacheValuesForKey(int key, boolean expectEquality) {
      List<Map<Address, InternalCacheValue<Object>>> cacheVersions = new ArrayList<>();
      for (int i = 0; i < numMembersInCluster; i++)
         cacheVersions.add(getAllVersions(i, key));

      boolean allowNullValues = key % NULL_VALUE_FREQUENCY == 0;
      int expectedValues = allowNullValues ? NUMBER_OF_OWNERS - 1 : NUMBER_OF_OWNERS;
      for (Map<Address, InternalCacheValue<Object>> map : cacheVersions) {
         assertEquals(map.toString(), NUMBER_OF_OWNERS, map.keySet().size());

         if (!allowNullValues)
            assertTrue("Version map contains null entries.", !map.values().contains(null));

         List<Object> values = map.values().stream()
               .filter(Objects::nonNull)
               .map(InternalCacheValue::getValue)
               .collect(Collectors.toList());
         assertEquals(values.toString(), expectedValues, values.size());

         if (expectEquality) {
            assertTrue("Inconsistent values returned, they should be the same", values.stream().allMatch(v -> v.equals(values.get(0))));
         } else {
            assertTrue("Expected inconsistent values, but all values were equal", map.values().stream().distinct().count() > 1);
         }
      }
   }

   private void createCluster() {
      waitForClusterToForm(CACHE_NAME);
      List<Address> members = getCache(0).getRpcManager().getMembers();

      TestingUtil.waitForNoRebalance(caches());
      assertTrue(members.size() == 4);
   }

   private void splitCluster() {
      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      TestingUtil.blockUntilViewsChanged(10000, 2, getCache(0), getCache(1), getCache(2), getCache(3));
   }

   private AdvancedCache<Object, Object> getCache(int index) {
      return advancedCache(index, CACHE_NAME);
   }

   private Stream<Map<Address, CacheEntry<Object, Object>>> getConflicts(int index) {
      return ConflictManagerFactory.get(getCache(index)).getConflicts();
   }

   private Map<Address, InternalCacheValue<Object>> getAllVersions(int index, Object key) {
      return ConflictManagerFactory.get(getCache(index)).getAllVersions(key);
   }

   private void dropClusteredGetCommands() {
      IntStream.range(0, numMembersInCluster).forEach(i -> wrapInboundInvocationHandler(getCache(i), DropClusteredGetCommandHandler::new));
   }

   private void delayStateTransferCompletion() {
      IntStream.range(0, numMembersInCluster).forEach(i -> wrapInboundInvocationHandler(getCache(i), DelayStateRequestCommandHandler::new));
   }

   private class DelayStateRequestCommandHandler implements PerCacheInboundInvocationHandler {
      final PerCacheInboundInvocationHandler delegate;

      DelayStateRequestCommandHandler(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof StateResponseCommand) {
            TestingUtil.sleepThread(STATE_TRANSFER_DELAY);
         }
         delegate.handle(command, reply, order);
      }
   }

   private class DropClusteredGetCommandHandler implements PerCacheInboundInvocationHandler {
      final PerCacheInboundInvocationHandler delegate;

      DropClusteredGetCommandHandler(PerCacheInboundInvocationHandler delegate) {
         this.delegate = delegate;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (!(command instanceof ClusteredGetCommand)) {
            delegate.handle(command, reply, order);
         }
      }
   }

   @Listener
   private class RehashListener {
      final AtomicBoolean isRehashInProgress = new AtomicBoolean();

      @DataRehashed
      @SuppressWarnings("unused")
      public void onDataRehashed(DataRehashedEvent event) {
         isRehashInProgress.compareAndSet(false, true);
      }
   }
}
