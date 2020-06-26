package org.infinispan.statetransfer;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Start cluster (A,B) redeploy after upgrade. Rolling upgrades always occur in the order B,A and A does not restart
 * until B has completed successfully.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@CleanupAfterMethod
@Test(groups = {"functional", "unstable"}, testName = "statetransfer.StatefulSetRollingUpgradeTest")
public class StatefulSetRollingUpgradeTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";
   private static final int NUM_ROLLING_UPGRADES = 4;

   private int numNodes;

   @Override
   public Object[] factory() {
      return new Object[]{
            new StatefulSetRollingUpgradeTest().setNumNodes(2),
            new StatefulSetRollingUpgradeTest().setNumNodes(3),
            new StatefulSetRollingUpgradeTest().setNumNodes(4),
            new StatefulSetRollingUpgradeTest().setNumNodes(5)
      };
   }

   private StatefulSetRollingUpgradeTest setNumNodes(int numNodes) {
      this.numNodes = numNodes;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"nodes"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{numNodes};
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));

      for (int id = 0; id < numNodes; id++)
         createStatefulCacheManager(id);

      waitForClusterToForm(CACHE_NAME);
   }

   public void testStateTransferRestart() {
      for (int i = 0; i < NUM_ROLLING_UPGRADES; i++) {
         for (int j = numNodes - 1; j > -1; j--) {
            manager(j).stop();
            cacheManagers.remove(j);
            waitForClusterToForm(CACHE_NAME);
            createStatefulCacheManager(j);
            waitForClusterToForm(CACHE_NAME);
         }
      }
   }

   private void createStatefulCacheManager(int id) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName() + File.separator + id);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      config.clustering()
            .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES)
            .stateTransfer().timeout(5 * numNodes, TimeUnit.SECONDS);
      EmbeddedCacheManager manager = createClusteredCacheManager(true, global, null, new TransportFlags().withFD(true));
      manager.defineConfiguration(CACHE_NAME, config.build());
      cacheManagers.add(id, manager);
   }
}
