package org.infinispan.globalstate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.testing.Testing.tmpDirectory;
import static org.infinispan.util.BlockingLocalTopologyManager.confirmTopologyUpdate;
import static org.infinispan.util.BlockingLocalTopologyManager.finishRebalance;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManager;

import java.util.EnumSet;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.BlockingLocalTopologyManager;
import org.infinispan.util.concurrent.BlockingManagerTestUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "globalstate.ConfigCacheOriginReadyTest")
public class ConfigCacheOriginReadyTest extends MultipleCacheManagersTest {

   // Both tests swap a global component (replaceTopologyManager) on the shared managers, which is
   // not restorable. Give each method a fresh cluster so the topology-manager replacement and the
   // caches created in one method do not leak into the next and make the result order-dependent.
   {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      for (int i = 0; i < 2; i++) {
         String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Integer.toString(i));
         GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
         global.globalState().enable().persistentLocation(stateDirectory)
               .configurationStorage(ConfigurationStorage.OVERLAY);
         addClusterEnabledCacheManager(global, null);
      }
      TestingUtil.blockUntilViewsReceived(30000, managers());
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
   }

   private static Configuration replicatedConfig() {
      return new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.REPL_SYNC)
            .stateTransfer().awaitInitialTransfer(true)
            .build();
   }

   /**
    * Publishing a cache into the CONFIG cache is acknowledged even while a remote member's
    * initial state transfer for that cache is held: the configuration is defined on the remote
    * member and the write completes, while the remote member's cache instance is still starting.
    * <p>
    * The write is driven through {@link GlobalConfigurationManager} and awaited on its
    * acknowledgement alone. Node 0 owns the cache before the write so node 1 is always the joining
    * member held by the gate, rather than racing to be the sole initial owner.
    */
   public void testCreateCacheReturnsWhileRemoteStateTransferBlocked() throws Exception {
      final String cacheName = "originReadyCreate";

      // Node 0 owns the cache and is stable before the CONFIG write, so node 1 is always the
      // joining member and the gate holds node 1's start rather than the sole initial owner's.
      manager(0).defineConfiguration(cacheName, replicatedConfig());
      TestingUtil.waitForNoRebalance(manager(0).getCache(cacheName));

      // Gate node 1's join. The gate blocks the rebalance phases, so node 1's cache start cannot complete until released.
      BlockingLocalTopologyManager topB = replaceTopologyManager(manager(1), cacheName);

      // Publish the cache into the CONFIG cache from node 0 and wait only on the write's acknowledgement,
      // not on node 1 starting its cache instance.
      GlobalConfigurationManager gcm0 = TestingUtil.extractGlobalComponent(manager(0), GlobalConfigurationManager.class);
      CompletionStage<Configuration> write = gcm0.getOrCreateCache(cacheName, replicatedConfig(),
            EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class));

      Future<Cache<Object, Object>> remoteCache;
      try {
         // The write at node 0 is acknowledged even though node 1's state transfer is gated.
         write.toCompletableFuture().get(10, TimeUnit.SECONDS);

         // Node 1 has the configuration defined, but its cache instance is not ready:
         // its initial state transfer is held at the gate, so getCache on node 1 does not return.
         assertThat(manager(1).cacheConfigurationExists(cacheName)).isTrue();
         remoteCache = fork(() -> manager(1).getCache(cacheName));
         assertThat(remoteCache.isDone()).isFalse();
      } finally {
         // Release node 1's state transfer so the cache finishes starting and teardown is clean.
         // Expecting the queued REBALANCE_START also confirms node 1 was genuinely held mid-join.
         confirmTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL, topB);
         finishRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL, topB);
         topB.stopBlocking();
      }

      // Once released, node 1's cache becomes ready and the cluster converges.
      assertThat(remoteCache.get(10, TimeUnit.SECONDS)).isNotNull();
      waitForClusterToForm(cacheName);
   }

   /**
    * A member that learns about a new cache through the global configuration state reports it as
    * existing ({@code cacheExists() == true}) as soon as the configuration is applied, even though the
    * cache start and its initial state transfer proceed asynchronously and the cache is not yet ready.
    * <p>
    * Node 1's blocking executor is held so its asynchronous start cannot run; registration must
    * therefore happen synchronously on the notification thread, independent of the blocking pool.
    */
   public void testCacheExistsOnRemoteMemberWhileStartIsPending() throws Exception {
      final String cacheName = "originReadyExists";

      // Node 0 owns the cache and is stable, so node 1 is the joining member whose start we hold.
      manager(0).defineConfiguration(cacheName, replicatedConfig());
      TestingUtil.waitForNoRebalance(manager(0).getCache(cacheName));
      assertThat(manager(1).cacheExists(cacheName)).isFalse();

      // Hold node 1's blocking executor: its asynchronous cache start is queued and cannot run.
      BlockingManagerTestUtil.HeldBlockingExecutor held =
            BlockingManagerTestUtil.holdBlockingExecutor(manager(1));

      GlobalConfigurationManager gcm0 = TestingUtil.extractGlobalComponent(manager(0), GlobalConfigurationManager.class);
      CompletionStage<Configuration> write = gcm0.getOrCreateCache(cacheName, replicatedConfig(),
            EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class));

      Future<Cache<Object, Object>> remoteCache;
      try {
         write.toCompletableFuture().get(10, TimeUnit.SECONDS);

         // The cache exists on node 1 even though its start is held in the blocked pool.
         assertThat(manager(1).cacheExists(cacheName)).isTrue();

         // It is not ready: getCache does not return while the start is held.
         remoteCache = fork(() -> manager(1).getCache(cacheName));
         assertThat(remoteCache.isDone()).isFalse();
      } finally {
         // Run the held start so the cache finishes and teardown is clean.
         held.release();
      }

      assertThat(remoteCache.get(10, TimeUnit.SECONDS)).isNotNull();
      waitForClusterToForm(cacheName);
   }

   /**
    * A {@code getCache} on a joining member does not return until that member's own initial state
    * transfer has completed, honouring {@code awaitInitialTransfer(true)}.
    * <p>
    * The cache is defined directly on both members ({@code defineConfiguration}) rather than
    * through the CONFIG cache, so the joining member's start is driven by a single, observable
    * {@code getCache}. Both members' topology managers are gated and advanced together.
    */
   public void testGetCacheHonoursAwaitInitialTransferOnRemoteMember() throws Exception {
      final String cacheName = "originReadyAwait";
      Configuration cfg = replicatedConfig();

      // Node 0 owns the cache and is stable before any gating; only the later rebalance
      // triggered by node 1 joining is blocked.
      manager(0).defineConfiguration(cacheName, cfg);
      TestingUtil.waitForNoRebalance(manager(0).getCache(cacheName));
      assertThat(manager(1).cacheExists(cacheName)).isFalse();

      // Gate the join rebalance on both members and advance them in lockstep.
      BlockingLocalTopologyManager topA = replaceTopologyManager(manager(0), cacheName);
      BlockingLocalTopologyManager topB = replaceTopologyManager(manager(1), cacheName);

      manager(1).defineConfiguration(cacheName, cfg);
      // getCache blocks on awaitInitialTransfer, so drive it off-thread.
      Future<Cache<String, String>> remoteCache = fork(() -> manager(1).getCache(cacheName));

      try {
         // Hold node 1 at the first rebalance phase. Its initial state transfer only completes at
         // NO_REBALANCE, so while it is held here getCache must not have returned.
         confirmTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL, topA, topB);
         assertThat(remoteCache.isDone()).isFalse();
      } finally {
         // Release the remaining phases through NO_REBALANCE so the initial state transfer
         // completes and getCache can return.
         finishRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL, topA, topB);
         topA.stopBlocking();
         topB.stopBlocking();
      }

      // Only once the initial state transfer completes does getCache return the started cache.
      assertThat(remoteCache.get(10, TimeUnit.SECONDS)).isNotNull();

      waitForClusterToForm(cacheName);
   }
}
