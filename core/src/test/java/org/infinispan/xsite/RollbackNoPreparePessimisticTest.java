package org.infinispan.xsite;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test (groups = "xsite", testName = "xsite.RollbackNoPreparePessimisticTest")
public class RollbackNoPreparePessimisticTest extends RollbackNoPrepareOptimisticTest {

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getPessimisticDistTxConnfig();
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getPessimisticDistTxConnfig();
   }

   private ConfigurationBuilder getPessimisticDistTxConnfig() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return cb;
   }
}
