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

   private Object k0;
   private Object k1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = createConfiguration();

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(config);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      waitForClusterToForm();

      k0 = new MagicKey(cache(0));
      k1 = new MagicKey(cache(1));
   }

   protected ConfigurationBuilder createConfiguration() {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      config
         .unsafe().unreliableReturnValues(true)
         .clustering().hash().numOwners(1)
         .deadlockDetection().enable()
         .transaction().lockingMode(LockingMode.PESSIMISTIC);
      return config;
   }

   public void testSymmetricDeadlock() throws SystemException {
      testSymmetricDld(k0, k1);
   }
}
