package org.infinispan.distexec.mapreduce;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying the Map Reduce execution for the Topology Aware nodes.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "distexec.mapreduce.TopologyAwareTwoNodesMapReduceTest")
public class TopologyAwareTwoNodesMapReduceTest extends SimpleTwoNodesMapReduceTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder =
            getDefaultClusteredCacheConfig(getCacheMode(), false);

      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("a").rackId("b").siteId("test1");

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(globalConfigurationBuilder,
                                                                                     builder);
      cm1.defineConfiguration(cacheName(), builder.build());
      cacheManagers.add(cm1);

      globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("b").rackId("b").siteId("test2");
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfigurationBuilder,
                                                                                     builder);

      cm2.defineConfiguration(cacheName(), builder.build());
      cacheManagers.add(cm2);

      waitForClusterToForm(cacheName());
   }

   //Overriding these tests with empty body due to additional cache that is created during these tests.
   @Override
   public void testEnsureProperCacheState() throws Exception {

   }

   @Override
   public void testEnsureProperCacheStateMode() {

   }

}