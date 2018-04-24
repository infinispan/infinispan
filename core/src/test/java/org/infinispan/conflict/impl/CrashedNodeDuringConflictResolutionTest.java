package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.conflict.ConflictManager;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;


/**
 * 1. Partition cluster
 * 2. When coordinator sends InboundTransferTask with segment for DURING_CR_CRASH_KEY crash the node
 * 3. CR should hang until a new view is received
 * 4. The previous CR should be cancelled and restarted with the crashed node removed
 * 5. All keys should have the resolved value from the EntryMergePolicy
 */
@Test(groups = "functional", testName = "org.infinispan.conflict.impl.CrashedNodeDuringConflictResolutionTest")
public class CrashedNodeDuringConflictResolutionTest extends BaseMergePolicyTest {

   private static final Log log = LogFactory.getLog(CrashedNodeDuringConflictResolutionTest.class);
   private static boolean trace = log.isTraceEnabled();

   private static final String PARTITION_0_VAL = "A";
   private static final String PARTITION_1_VAL = "B";
   private static final String BEFORE_CR_CRASH_KEY = "BEFORE_CR_CRASH";
   private static final String DURING_CR_CRASH_KEY = "DURING_CR_CRASH";
   private static final String AFTER_CR_RESTART_KEY = "AFTER_CR_CRASH";
   private static final String RESOLVED_VALUE = "RESOLVED";
   private static final String[] ALL_KEYS = new String[] {BEFORE_CR_CRASH_KEY, DURING_CR_CRASH_KEY, AFTER_CR_RESTART_KEY};
   private static final EntryMergePolicy POLICY = (preferredEntry, otherEntries) -> {
      Object key = preferredEntry != null ? preferredEntry.getKey() : ((CacheEntry)otherEntries.get(0)).getKey();
      return new ImmortalCacheEntry(key, RESOLVED_VALUE);
   };
   private static final KeyPartitioner PARTITIONER = new TestKeyPartioner();

   public CrashedNodeDuringConflictResolutionTest() {
      super(DIST_SYNC, null, new int[]{0, 1}, new int[]{2, 3});
      this.mergePolicy = POLICY;
      this.valueAfterMerge = RESOLVED_VALUE;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = cacheConfiguration();
      dcc.clustering()
            .cacheMode(cacheMode).partitionHandling().whenSplit(partitionHandling).mergePolicy(mergePolicy)
            .hash().keyPartitioner(PARTITIONER);
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm();
   }

   @Override
   protected void beforeSplit() {
      // Ignore
   }

   @Override
   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) {
      for (String key : ALL_KEYS) {
         cache(p0.node(0)).put(key, PARTITION_0_VAL);
         cache(p1.node(0)).put(key, PARTITION_1_VAL);
      }

      for (String key : ALL_KEYS) {
         assertCacheGet(key, PARTITION_0_VAL, p0.getNodes());
         assertCacheGet(key, PARTITION_1_VAL, p1.getNodes());
      }
   }

   @Override
   protected void performMerge() throws Exception {
      CompletableFuture<StateRequestCommand> blockedStateRequest = createStateRequestFuture();

      for (String key : ALL_KEYS) {
         assertCacheGet(key, PARTITION_0_VAL, p0.getNodes());
         assertCacheGet(key, PARTITION_1_VAL, p1.getNodes());
      }

      partition(0).merge(partition(1), false);
      blockedStateRequest.get(60, TimeUnit.SECONDS);

      if (trace) log.trace("crashCacheManager(2)");
      TestingUtil.crashCacheManagers(manager(2));
      // Once the JGroups view has been updated to remove manager(index), then the CR should be restarted when the
      // coordinator continues to recover the cluster state
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(3));
   }

   @Override
   protected void afterConflictResolutionAndMerge() {
      ConflictManager cm = conflictManager(0);
      assertFalse(cm.isConflictResolutionInProgress());
      for (String key : ALL_KEYS) {
         Map<Address, InternalCacheValue> versionMap = cm.getAllVersions(key);
         assertNotNull(versionMap);
         assertEquals("Versions: " + versionMap, numberOfOwners, versionMap.size());
         String message = String.format("Key=%s. VersionMap: %s", key, versionMap);
         for (InternalCacheValue icv : versionMap.values()) {
            assertNotNull(message, icv);
            assertNotNull(message, icv.getValue());
            assertEquals(message, valueAfterMerge, icv.getValue());
         }
      }
      assertEquals(0, cm.getConflicts().peek(m -> log.errorf("Conflict: " + m)).count());
   }

   private CompletableFuture<StateRequestCommand> createStateRequestFuture() {
      int segment = PARTITIONER.getSegment(DURING_CR_CRASH_KEY);
      CompletableFuture<StateRequestCommand> future = new CompletableFuture<>();
      wrapInboundInvocationHandler(cache(2), handler -> new CompleteFutureOnStateRequestHandler(handler, segment, manager(2), future));
      return future;
   }

   private class CompleteFutureOnStateRequestHandler implements PerCacheInboundInvocationHandler {
      final PerCacheInboundInvocationHandler delegate;
      final int segment;
      final EmbeddedCacheManager manager;
      final CompletableFuture<StateRequestCommand> future;

      CompleteFutureOnStateRequestHandler(PerCacheInboundInvocationHandler delegate, int segment, EmbeddedCacheManager manager,
                                          CompletableFuture<StateRequestCommand> future) {
         this.delegate = delegate;
         this.segment = segment;
         this.manager = manager;
         this.future = future;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof StateRequestCommand) {
            StateRequestCommand src = (StateRequestCommand) command;
            if (src.getSegments().contains(segment)) {
               future.complete(src);
               return;
            }
         }
         delegate.handle(command, reply, order);
      }
   }

   public static class TestKeyPartioner implements KeyPartitioner {

      private KeyPartitioner delegate = new HashFunctionPartitioner();

      @Override
      public void init(HashConfiguration configuration) {
         delegate.init(configuration);
      }

      @Override
      public int getSegment(Object key) {
         if (key instanceof String) {
            String keyString = (String) key;
            switch (keyString) {
               case BEFORE_CR_CRASH_KEY:
                  return 10;
               case DURING_CR_CRASH_KEY:
                  return 20;
               case AFTER_CR_RESTART_KEY:
                  return 30;
            }
         }
         return delegate.getSegment(key);
      }
   }
}
