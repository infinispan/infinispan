package org.infinispan.globalstate;

import static org.infinispan.test.Mocks.AFTER_INVOCATION;
import static org.infinispan.test.Mocks.AFTER_RELEASE;
import static org.infinispan.test.Mocks.BEFORE_INVOCATION;
import static org.infinispan.test.Mocks.BEFORE_RELEASE;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.AbstractControlledLocalTopologyManager;
import org.testng.annotations.Test;

/**
 * A node without persistent state joins a cache that restarts from persistent state.
 * <p>
 * The coordinator rejects the joiner until the restored members confirm the stable topology, and the joiner is
 * responsible for retrying. The joiner first asks before the topology is restored, and retries while the confirmations
 * are still pending. It must still join once every restored member confirms.
 */
@Test(testName = "globalstate.StatelessJoinerDuringStableTopologyConfirmationTest", groups = "functional")
public class StatelessJoinerDuringStableTopologyConfirmationTest extends AbstractGlobalStateRestartTest {

   private CacheMode cacheMode;

   @Override
   protected int getClusterSize() {
      return 2;
   }

   @Override
   protected void applyCacheManagerClusteringConfiguration(ConfigurationBuilder config) {
      config.clustering().cacheMode(cacheMode);
   }

   public void testJoinerRetriesWhileStableTopologyConfirmationIsPending() throws Throwable {
      createInitialCluster();

      // Shutdown the cache cluster-wide, so A and B restart from their persistent state.
      cache(0, CACHE_NAME).shutdown();
      TestingUtil.killCacheManagers(cacheManagers);
      cacheManagers.clear();

      // Index 0 (A) is the coordinator, 1 (B) has state, and 2 (the joiner) has no state.
      createStatefulCacheManager("A", false);
      createStatefulCacheManager("B", false);
      createStatefulCacheManager("@", true);
      TestingUtil.blockUntilViewsReceived(15_000, manager(0), manager(1), manager(2));
      assertTrue(manager(0).isCoordinator());

      // B holds the restored stable topology, so the coordinator keeps waiting for its confirmation.
      HoldRestoredStableTopology holdOnB = TestingUtil.wrapGlobalComponent(manager(1), LocalTopologyManager.class,
            HoldRestoredStableTopology::new, true);

      // A starts first and waits for B, so the previous topology is not restored yet.
      cache(0, CACHE_NAME);

      // Joins are not blocked, the check point only counts the join requests the coordinator answered.
      CheckPoint joins = Mocks.blockInboundGlobalCommandExecution(manager(0),
            rc -> rc instanceof CacheJoinCommand cjc && CACHE_NAME.equals(cjc.getCacheName()));
      joins.triggerForever(BEFORE_RELEASE);
      joins.triggerForever(AFTER_RELEASE);

      // The joiner asks before the topology is restored and receives an empty response.
      Future<Cache<Object, Object>> joinerCreate = fork(() -> cache(2, CACHE_NAME));
      joins.awaitStrict(BEFORE_INVOCATION, 10, TimeUnit.SECONDS);
      joins.awaitStrict(AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // B joins, the coordinator restores the topology and broadcasts it as the stable topology.
      Future<Cache<Object, Object>> statefulCreate = fork(() -> cache(1, CACHE_NAME));
      joins.awaitStrict(AFTER_INVOCATION, 10, TimeUnit.SECONDS);
      holdOnB.received.get(10, TimeUnit.SECONDS);

      // The restored stable topology reached the joiner, which retries while B's confirmation is still pending.
      joins.awaitStrict(AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // B confirms, and the joiner must eventually join.
      holdOnB.release.complete(null);
      statefulCreate.get(10, TimeUnit.SECONDS);
      joinerCreate.get(10, TimeUnit.SECONDS);

      eventually(() -> cacheManagers.stream()
            .map(ecm -> TestingUtil.extractGlobalComponent(ecm, LocalTopologyManager.class))
            .noneMatch(ltm -> ltm.isCacheRecoveringShutdown(CACHE_NAME)), 30, TimeUnit.SECONDS);
      waitForClusterToForm(CACHE_NAME);
      checkData();
   }

   /**
    * Holds the restored stable topology of {@link #CACHE_NAME} until the test releases it. The node only confirms
    * the stable topology to the coordinator after handling it.
    */
   static final class HoldRestoredStableTopology extends AbstractControlledLocalTopologyManager {
      final CompletableFuture<Void> received = new CompletableFuture<>();
      final CompletableFuture<Void> release = new CompletableFuture<>();

      HoldRestoredStableTopology(LocalTopologyManager delegate) {
         super(delegate);
      }

      @Override
      public CompletionStage<Void> handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology,
                                                              Address sender, int viewId) {
         if (CACHE_NAME.equals(cacheName) && cacheTopology != null && cacheTopology.wasTopologyRestoredFromState()) {
            received.complete(null);
            return release.thenCompose(ignore -> super.handleStableTopologyUpdate(cacheName, cacheTopology, sender, viewId));
         }
         return super.handleStableTopologyUpdate(cacheName, cacheTopology, sender, viewId);
      }
   }

   private StatelessJoinerDuringStableTopologyConfirmationTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new StatelessJoinerDuringStableTopologyConfirmationTest().withCacheMode(CacheMode.DIST_SYNC),
            new StatelessJoinerDuringStableTopologyConfirmationTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected String parameters() {
      return String.format("[cacheMode=%s]", cacheMode);
   }
}
