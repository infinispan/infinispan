package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.TotalOrderAsyncReplExtendedStatisticTest")
public class TotalOrderAsyncReplExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected TotalOrderAsyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_ASYNC, false, false, true);
   }
}
