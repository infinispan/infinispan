package org.infinispan.notifications.cachelistener;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;


@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplInitialTransferLocalTest")
public class CacheNotifierImplInitialTransferLocalTest extends BaseCacheNotifierImplInitialTransferTest {
   protected CacheNotifierImplInitialTransferLocalTest() {
      super(CacheMode.LOCAL);
   }
}
