package org.infinispan.tx.dld;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import javax.transaction.SystemException;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test (groups = "functional", testName = "tx.dld.DldPessimisticLockingDistributedTest")
public class DldPessimisticLockingDistributedTest extends BaseDldPessimisticLockingTest {

   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config
         .unsafe().unreliableReturnValues(true)
         .clustering().hash().numOwners(1)
         .deadlockDetection().enable()
         .transaction().lockingMode(LockingMode.PESSIMISTIC);
      return config;
   }

}
