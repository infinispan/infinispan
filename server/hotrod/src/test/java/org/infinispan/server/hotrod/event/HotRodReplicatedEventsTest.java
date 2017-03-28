package org.infinispan.server.hotrod.event;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodReplicatedEventsTest")
public class HotRodReplicatedEventsTest extends AbstractHotRodClusterEventsTest {
   public HotRodReplicatedEventsTest() {
      cacheMode = CacheMode.REPL_SYNC;
   }
}
