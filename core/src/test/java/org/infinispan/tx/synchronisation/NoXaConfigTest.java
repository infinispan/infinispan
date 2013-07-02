package org.infinispan.tx.synchronisation;

import org.infinispan.config.Configuration;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.synchronisation.NoXaConfigTest")
public class NoXaConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml("configs/no-xa-config.xml");
   }

   public void testConfig() {
      assert cacheManager.getCache("syncEnabled").getConfiguration().isUseSynchronizationForTransactions();
      assert cacheManager.getCache("notSpecified").getConfiguration().isUseSynchronizationForTransactions();

      cacheManager.getCache("syncAndRecovery");
   }

   public void testConfigOverride() {
      Configuration configuration = getDefaultStandaloneConfig(true);
      configuration.fluent().transaction().useSynchronization(true);
      cacheManager.defineConfiguration("newCache", configuration);
      assert cacheManager.getCache("newCache").getConfiguration().isUseSynchronizationForTransactions();
   }
}
