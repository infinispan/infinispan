package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronization.NoXaConfigTest")
public class NoXaConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager("configs/no-xa-config.xml");
   }

   public void testConfig() {
      assert cacheManager.getCache("syncEnabled").getConfiguration().isUseSynchronizationForTransactions();
      assert !cacheManager.getCache("notSpecified").getConfiguration().isUseSynchronizationForTransactions();

      try {
         cacheManager.getCache("syncAndRecovery");
         assert false;
      } catch (ConfigurationException e) {
         //expected
      }
   }

   public void testConfigOverride() {
      Configuration configuration = getDefaultStandaloneConfig(true);
      configuration.configureTransaction().useSynchronization(true);
      cacheManager.defineConfiguration("newCache", configuration);
      assert cacheManager.getCache("newCache").getConfiguration().isUseSynchronizationForTransactions();
   }
}
