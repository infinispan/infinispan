package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadReplOptimisticTest")
@CleanupAfterMethod
public class PutForExternalReadReplOptimisticTest extends PutForExternalReadTest {

   protected ConfigurationBuilder createCacheConfigBuilder() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }
}
