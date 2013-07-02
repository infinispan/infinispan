package org.infinispan.tx.dld;

import org.infinispan.config.Configuration;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
      Configuration config = createConfiguration();

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(config);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(config);
      registerCacheManager(cm1);
      registerCacheManager(cm2);
      waitForClusterToForm();

      k0 = new MagicKey(cache(0));
      k1 = new MagicKey(cache(1));
   }

   protected Configuration createConfiguration() {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC, true);
      config.setUnsafeUnreliableReturnValues(true);
      config.setNumOwners(1);
      config.setEnableDeadlockDetection(true);
      config.setUseEagerLocking(true);
      return config;
   }

   public void testSymmetricDeadlock() throws SystemException {
      testSymmetricDld(k0, k1);
   }
}
