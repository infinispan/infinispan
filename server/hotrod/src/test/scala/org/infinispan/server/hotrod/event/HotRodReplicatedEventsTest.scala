package org.infinispan.server.hotrod.event

import org.infinispan.configuration.cache.CacheMode
import org.testng.annotations.Test

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"), testName = "server.hotrod.event.HotRodReplicatedEventsTest")
class HotRodReplicatedEventsTest extends AbstractHotRodClusterEventsTest {
   override protected def cacheMode: CacheMode = CacheMode.REPL_SYNC
}
