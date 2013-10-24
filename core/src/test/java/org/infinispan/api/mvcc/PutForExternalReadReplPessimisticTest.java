package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadReplPessimisticTest")
@CleanupAfterMethod
public class PutForExternalReadReplPessimisticTest extends PutForExternalReadTest {

   @Override
   protected ConfigurationBuilder createCacheConfigBuilder() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.clustering().hash().numSegments(4);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return c;
   }
}
