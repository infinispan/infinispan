package org.infinispan.invalidation;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Test preloading with an invalidation cache.
 *
 * @author Dan Berindei
 * @since 10.0
 */
@Test(groups = "functional", testName = "invalidation.InvalidationPreloadTest")
@CleanupAfterMethod
public class InvalidationPreloadTest extends MultipleCacheManagersTest {

   private boolean shared;

   @Override
   public Object[] factory() {
      return new Object[]{
            new InvalidationPreloadTest().shared(true).transactional(false),
            new InvalidationPreloadTest().shared(true).transactional(true),
            new InvalidationPreloadTest().shared(false).transactional(false),
            new InvalidationPreloadTest().shared(false).transactional(true),
      };
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"tx", "shared"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{transactional, shared};
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // The managers are created in the test method
   }

   public void testPreloadOnStart() throws PersistenceException {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      configuration.transaction().transactionMode(transactionMode());
      DummyInMemoryStoreConfigurationBuilder dimsConfiguration =
            configuration.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      dimsConfiguration.storeName(getTestName()).preload(true).shared(shared);

      EmbeddedCacheManager cm1 =
            addClusterEnabledCacheManager(TestDataSCI.INSTANCE, configuration, new TransportFlags().withFD(false));
      Cache<String, String> c1 = cm1.getCache();

      c1.put("k" + 0, "v" + 0);

      assertKeys(c1);

      cm1.stop();

      EmbeddedCacheManager cm2 =
            addClusterEnabledCacheManager(TestDataSCI.INSTANCE, configuration, new TransportFlags().withFD(false));
      Cache<String, String> c2 = cm2.getCache();

      assertKeys(c2);

      if (shared) {
         EmbeddedCacheManager cm3 =
               addClusterEnabledCacheManager(TestDataSCI.INSTANCE, configuration, new TransportFlags().withFD(false));
         Cache<String, String> c3 = cm3.getCache();

         assertKeys(c3);
      }
   }

   private void assertKeys(Cache<String, String> cache) {
      DataContainer<String, String> dc = cache.getAdvancedCache().getDataContainer();
      assertEquals(1, dc.size());

      DummyInMemoryStore cs = TestingUtil.getFirstStore(cache);
      assertEquals(1, cs.size());
   }

   private InvalidationPreloadTest shared(boolean shared) {
      this.shared = shared;
      return this;
   }
}
