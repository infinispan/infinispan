package org.infinispan.server.hotrod.event;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodDistributedEventsTest")
public class HotRodDistributedEventsTest extends AbstractHotRodClusterEventsTest {
   public HotRodDistributedEventsTest() {
      cacheMode = CacheMode.DIST_SYNC;
   }
}
