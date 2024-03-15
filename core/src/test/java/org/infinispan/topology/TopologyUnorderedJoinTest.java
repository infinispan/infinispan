package org.infinispan.topology;

import static org.infinispan.test.Mocks.AFTER_INVOCATION;
import static org.infinispan.test.Mocks.AFTER_RELEASE;
import static org.infinispan.test.Mocks.BEFORE_INVOCATION;
import static org.infinispan.test.Mocks.BEFORE_RELEASE;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.Cache;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

@Test(testName = "topology.TopologyUnorderedJoinTest", groups = "functional")
public class TopologyUnorderedJoinTest extends MultipleCacheManagersTest {

   private final int dataSize = 100;
   private final String cacheName = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(defaultGlobalConfig(), defaultCacheConfig(), 2);
      for (EmbeddedCacheManager manager : managers()) {
         defineConfiguration(manager);
      }
   }

   private GlobalConfigurationBuilder defaultGlobalConfig() {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().distributedSyncTimeout(15, TimeUnit.SECONDS);
      return gcb;
   }

   private ConfigurationBuilder defaultCacheConfig() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cb.clustering().stateTransfer().timeout(30, TimeUnit.SECONDS);
      return cb;
   }

   private EmbeddedCacheManager addNewMember() {
      return addClusterEnabledCacheManager(defaultGlobalConfig(), defaultCacheConfig());
   }

   private void defineConfiguration(EmbeddedCacheManager ecm) {
      ecm.defineConfiguration(cacheName, defaultCacheConfig().build());
   }

   public void testDelayJoinResponseAfterRebalanceStart() throws Exception {
      // Create the cache on the initial two nodes and populate.
      waitForClusterToForm(cacheName);
      populateCache();

      // Replace to block on the join request.
      // We allow the request to process, and unlock only after another node joins and the view updates.
      AtomicBoolean onlyOnce = new AtomicBoolean(true);
      CheckPoint joinPoint =  Mocks.blockInboundGlobalCommandExecution(findCoordinator(), rc -> {
         if (rc instanceof CacheJoinCommand cjc) {
            return cjc.getCacheName().equals(cacheName) && onlyOnce.getAndSet(false);
         }
         return false;
      });

      // Add new node.
      // We add an interceptor to the rebalance start command to the joiner.
      EmbeddedCacheManager joiner = addNewMember();
      defineConfiguration(joiner);

      // Request cache asynchronously, since we block the initial commands.
      Future<Cache<String, String>> future = fork(() -> joiner.getCache(cacheName));

      // We allow the join request to process.
      joinPoint.awaitStrict(BEFORE_INVOCATION, 10, TimeUnit.SECONDS);
      joinPoint.trigger(BEFORE_RELEASE);

      // Wait the join to process on the coordinator and the rebalance request arrive at the joiner.
      joinPoint.awaitStrict(AFTER_INVOCATION, 10, TimeUnit.SECONDS);

      // New node joins, this causes the view to update.
      // No need to request the cache now, this is enough to cause a view change.
      EmbeddedCacheManager ecm = addNewMember();
      defineConfiguration(ecm);

      // Wait view is installed on all nodes.
      // Since the view is global, we can utilize the default cache instead here.
      // This way, we don't block with the second joiner also creating the cache.
      TestingUtil.blockUntilViewsReceived(15_000, caches());

      // Release the join on the coordinator.
      // The joiner will receive a response and retry.
      joinPoint.trigger(AFTER_RELEASE);

      // Wait to the state transfer to finish.
      // This also creates the cache on the second joiner.
      waitForClusterToForm(cacheName);
      future.get(10, TimeUnit.SECONDS);

      // Assert the data is available from all nodes.
      assertCacheData();
   }

   private void populateCache() {
      Cache<String, String> cache = cache(0, cacheName);
      IntStream.range(0, dataSize).parallel()
            .forEach(i -> cache.put("key-" + i, "value-" + i));
   }

   private void assertCacheData() {
      // Use soft assertions to check all caches and all values.
      SoftAssertions sa = new SoftAssertions();
      for (int m = 0; m < managers().length; m++) {
         Cache<String, String> cache = cache(m, cacheName);
         int size = cache.size();
         sa.assertThat(size)
               .withFailMessage(String.format("Cache %d has %d/%d entries", m, size, dataSize))
               .isEqualTo(dataSize);

         for (int i = 0; i < dataSize; i++) {
            sa.assertThat(cache.get("key-" + i)).isEqualTo("value-" + i);
         }
      }

      sa.assertAll();
   }

   private EmbeddedCacheManager findCoordinator() {
      return manager(findCoordinatorIndex());
   }

   private int findCoordinatorIndex() {
      for (int i = 0; i < managers().length; i++) {
         if (manager(i).isCoordinator()) return i;
      }

      throw new IllegalStateException("Coordinator node not found");
   }
}
