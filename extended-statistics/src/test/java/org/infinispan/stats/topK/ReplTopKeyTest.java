package org.infinispan.stats.topK;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusterTopKeyTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.topK.ReplTopKeyTest")
public class ReplTopKeyTest extends BaseClusterTopKeyTest {
   public ReplTopKeyTest() {
      super(CacheMode.REPL_SYNC, 2);
   }
}
