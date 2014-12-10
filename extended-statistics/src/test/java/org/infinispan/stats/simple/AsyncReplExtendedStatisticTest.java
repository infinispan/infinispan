package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseNonTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.AsyncReplExtendedStatisticTest")
public class AsyncReplExtendedStatisticTest extends BaseNonTotalOrderClusteredExtendedStatisticsTest {

   public AsyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_ASYNC, false, false);
   }
}
