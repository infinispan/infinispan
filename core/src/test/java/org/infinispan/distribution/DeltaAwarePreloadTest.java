package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.atomic.DeltaAware;
import org.infinispan.atomic.TestDeltaAware;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

/**
 * Test for preloading delta aware cache values
 *
 * @author gustavonalle
 * @since 7.0
 */
public class DeltaAwarePreloadTest extends MultipleCacheManagersTest {

   private static final int CLUSTER_SIZE = 1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      c.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(c.persistence()).storeName(getClass().getSimpleName())).preload(true);
      createCluster(c, CLUSTER_SIZE);
      waitForClusterToForm();
   }

   @Test
   public void testPreloadOnStart() throws PersistenceException {
      Cache<Object, Object> cache = caches().get(0);
      cache.put(1, new TestDeltaAware());
      cache.stop();
      cache.start();

      Object deltaAware = cache.get(1);

      assertTrue(deltaAware instanceof DeltaAware);

   }

}
