package org.infinispan.distribution;

import static org.testng.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.atomic.TestDeltaAware;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class BaseDeltaAwarePreloadTest extends MultipleCacheManagersTest {

   protected static final int CLUSTER_SIZE = 2;

   abstract boolean isTx();

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getConfig(isTx()), CLUSTER_SIZE);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder getConfig(boolean tx) throws Throwable {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, tx);
      c.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(c.persistence()).storeName(getClass().getSimpleName())).preload(true);
      return c;
   }

   @Test
   public void testPreloadOnStart() throws PersistenceException {
      Cache<Object, Object> cache = caches().get(0);
      TestDeltaAware value = new TestDeltaAware();
      value.setFirstComponent("1st");
      value.setSecondComponent("2nd");
      cache.put(1, value);
      cache.stop();
      cache.start();

      TestDeltaAware deltaAware = (TestDeltaAware) cache.get(1);

      assertEquals("1st", deltaAware.getFirstComponent());
      assertEquals("2nd", deltaAware.getSecondComponent());
   }

}
