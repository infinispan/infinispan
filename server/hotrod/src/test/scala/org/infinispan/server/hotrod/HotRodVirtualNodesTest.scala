package org.infinispan.server.hotrod

import org.infinispan.config.Configuration
import org.testng.annotations.Test

/**
 * Tests HotRod logic when underlying cache is configured with virtual nodes
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodVirtualNodesTest")
class HotRodVirtualNodesTest extends HotRodDistributionTest {

   override protected def createCacheConfig: Configuration =
      super.createCacheConfig.fluent.clustering.hash.numVirtualNodes(10).build

}