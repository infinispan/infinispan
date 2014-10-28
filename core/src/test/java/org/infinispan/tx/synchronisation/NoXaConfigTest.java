package org.infinispan.tx.synchronisation;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

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
      assertTrue(cacheManager.getCache("syncEnabled").getCacheConfiguration().transaction().useSynchronization());
      assertFalse(cacheManager.getCache("notSpecified").getCacheConfiguration().transaction().useSynchronization());

      cacheManager.getCache("syncAndRecovery");
   }

   public void testConfigOverride() {
      ConfigurationBuilder configuration = getDefaultStandaloneCacheConfig(true);
      configuration.transaction().useSynchronization(true);
      cacheManager.defineConfiguration("newCache", configuration.build());
      assertTrue(cacheManager.getCache("newCache").getCacheConfiguration().transaction().useSynchronization());
   }
}
