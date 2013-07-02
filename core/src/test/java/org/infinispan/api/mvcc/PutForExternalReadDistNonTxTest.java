package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadDistNonTxTest")
@CleanupAfterMethod
public class PutForExternalReadDistNonTxTest extends PutForExternalReadTest {

   @Override
   protected ConfigurationBuilder createCacheConfigBuilder() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      c.clustering().hash().numOwners(100).numSegments(4).l1().disable();
      return c;
   }

   @Override
   public void testCacheModeLocalInTx(Method m) {
      // not applicable in non-tx mode
   }

   @Override
   public void testMemLeakOnSuspendedTransactions() {
      // not applicable in non-tx mode
   }

   @Override
   public void testTxSuspension() {
      // not applicable in non-tx mode
   }
}
