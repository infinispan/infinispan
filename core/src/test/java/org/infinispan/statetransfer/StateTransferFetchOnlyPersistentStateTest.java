package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * StateTransferFetchOnlyPersistentStateTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferFetchOnlyPersistentStateTest")
public class StateTransferFetchOnlyPersistentStateTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cfg = createConfiguration(1);
      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration("onlyFetchPersistent", cfg.build());
   }

   private ConfigurationBuilder createConfiguration(int id) {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cfg.clustering().stateTransfer().fetchInMemoryState(false);

      DummyInMemoryStoreConfigurationBuilder dimcs = new DummyInMemoryStoreConfigurationBuilder(cfg.persistence());
      dimcs.storeName("store id: " + id);
      dimcs.fetchPersistentState(true).shared(false);
      cfg.persistence().addStore(dimcs);

      return cfg;
   }

   public void test000(Method m) {
      final String theKey = "k-" + m.getName();
      final String theValue = "v-" + m.getName();

      Cache cache1 = cache(0, "onlyFetchPersistent");
      assert !cache1.getCacheConfiguration().clustering().stateTransfer().fetchInMemoryState();
      cache1.put(theKey, theValue);

      ConfigurationBuilder cfg2 = createConfiguration(2);
      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager();
      cm2.defineConfiguration("onlyFetchPersistent", cfg2.build());

      Cache cache2 = cache(1, "onlyFetchPersistent");
      assert !cache2.getCacheConfiguration().clustering().stateTransfer().fetchInMemoryState();
      assert cache2.containsKey(theKey);
      assert cache2.get(theKey).equals(theValue);
   }

}
