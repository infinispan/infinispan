package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadReplTotalOrderTest")
@CleanupAfterMethod
public class PutForExternalReadReplTotalOrderTest extends PutForExternalReadTest {

   protected ConfigurationBuilder createCacheConfigBuilder() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.clustering().hash().numSegments(4);
      c.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .recovery().disable();
      return c;
   }

   @Override
   @Test(enabled = false, description = "Exception suppression doesn't work with TO, see ISPN-3300")
   public void testExceptionSuppression() throws Exception {
      super.testExceptionSuppression();
   }
}
