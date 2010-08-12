package org.infinispan.eviction;

import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "eviction.EvictionWithPassivationConfigurationTest")
public class EvictionWithPassivationConfigurationTest extends AbstractInfinispanTest {

   @Test (expectedExceptions = ConfigurationException.class)
   public void testConfig() {
      Configuration c = new Configuration();
      c.setEvictionStrategy(EvictionStrategy.LIRS);
      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.setPassivation(true);
      clmc.addCacheLoaderConfig(new DummyInMemoryCacheStore.Cfg());
      c.setCacheLoaderManagerConfig(clmc);
      new DefaultCacheManager(c);
   }
}
