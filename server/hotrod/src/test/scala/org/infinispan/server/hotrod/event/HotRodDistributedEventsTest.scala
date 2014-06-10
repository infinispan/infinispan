package org.infinispan.server.hotrod.event

import org.testng.annotations.Test
import org.infinispan.configuration.cache.CacheMode

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"), testName = "server.hotrod.event.HotRodDistributedEventsTest")
class HotRodDistributedEventsTest extends AbstractHotRodClusterEventsTest {
   override protected def cacheMode: CacheMode = CacheMode.DIST_SYNC
}
