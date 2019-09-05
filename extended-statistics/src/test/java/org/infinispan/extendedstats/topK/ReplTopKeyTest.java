package org.infinispan.extendedstats.topK;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.extendedstats.BaseClusterTopKeyTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.topK.ReplTopKeyTest")
public class ReplTopKeyTest extends BaseClusterTopKeyTest {
   public ReplTopKeyTest() {
      super(CacheMode.REPL_SYNC, 2);
   }
}
