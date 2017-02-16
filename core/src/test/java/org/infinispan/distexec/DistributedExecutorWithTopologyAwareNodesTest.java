package org.infinispan.distexec;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests dist.exec module using topology aware addresses.
 *
 * @author anna.manukyan
 * @since 5.2
 */
@Test(groups = "functional", testName = "distexec.DistributedExecutorWithTopologyAwareNodesTest")
public class DistributedExecutorWithTopologyAwareNodesTest extends DistributedExecutorTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);

      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder
               .defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("a").rackId("b").siteId("test1");

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfigurationBuilder, builder);
      cm1.defineConfiguration(cacheName(), builder.build());
      cacheManagers.add(cm1);

      globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfigurationBuilder.transport().machineId("b").rackId("b").siteId("test2");
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(
               globalConfigurationBuilder, builder);

      cm2.defineConfiguration(cacheName(), builder.build());
      cacheManagers.add(cm2);

      waitForClusterToForm(cacheName());
   }

   protected String cacheName() {
      return "DistributedExecutorWithTopologyAwareNodesTest";
   }
}
