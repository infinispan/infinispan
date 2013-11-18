package org.infinispan.stats.simple;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseClusteredExtendedStatisticTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.AsyncReplExtendedStatisticTest" )
public class AsyncReplExtendedStatisticTest extends BaseClusteredExtendedStatisticTest {

   protected AsyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_ASYNC, false, false, false);
   }

   @Test(enabled = false, description = "https://issues.jboss.org/browse/ISPN-3727")
   public void testReplaceWithOldVal() {
   }
}
