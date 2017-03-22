package org.infinispan.stats.simple;

import java.lang.reflect.Method;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stats.BaseNonTotalOrderClusteredExtendedStatisticsTest;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.simple.SyncReplExtendedStatisticTest")
public class SyncReplExtendedStatisticTest extends BaseNonTotalOrderClusteredExtendedStatisticsTest {

   public SyncReplExtendedStatisticTest() {
      super(CacheMode.REPL_SYNC);
   }

   @Test(groups = "unstable", description = "To be fixed by ISPN-6468")
   @Override
   public void testReplace(Method method) throws InterruptedException {
      super.testReplace(method);
   }
}
