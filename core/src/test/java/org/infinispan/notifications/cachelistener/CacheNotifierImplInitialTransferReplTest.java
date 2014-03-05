package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;


@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplInitialTransferReplTest")
public class CacheNotifierImplInitialTransferReplTest extends BaseCacheNotifierImplInitialTransferTest {
   protected CacheNotifierImplInitialTransferReplTest() {
      super(CacheMode.REPL_SYNC);
   }
}
