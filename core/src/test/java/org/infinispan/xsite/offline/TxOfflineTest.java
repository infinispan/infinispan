package org.infinispan.xsite.offline;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.offline.TxOfflineTest")
public class TxOfflineTest extends NonTxOfflineTest {

   public TxOfflineTest() {
      this.nrRpcPerPut = 1; //It's only the commit that fails (no prepare as by default we only replicate during commit)
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      ConfigurationBuilder dccc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dccc.transaction().useSynchronization(false).recovery().disable();
      return dccc;
   }
}
