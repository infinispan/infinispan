package org.infinispan.tx.locking;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional", testName = "tx.locking.PessimisticDistTxTest")
public class PessimisticDistTxTest extends PessimisticReplTxTest {

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(1);
      return builder;
   }
}
