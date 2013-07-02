package org.infinispan.loaders.jdbm.configuration;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.jdbm.JdbmCacheStoreConfig;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbm.configuration.ConfigurationTest")
public class ConfigurationTest {

   public void testBdbjeCacheStoreConfigurationAdaptor() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      b.loaders().addStore(JdbmCacheStoreConfigurationBuilder.class).location("/tmp/jdbm").expiryQueueSize(100).fetchPersistentState(true).async().enable();
      Configuration configuration = b.build();
      JdbmCacheStoreConfiguration store = (JdbmCacheStoreConfiguration) configuration.loaders().cacheLoaders().get(0);
      assert store.location().equals("/tmp/jdbm");
      assert store.expiryQueueSize() == 100;
      assert store.fetchPersistentState();
      assert store.async().enabled();

      b = new ConfigurationBuilder();
      b.loaders().addStore(JdbmCacheStoreConfigurationBuilder.class).read(store);
      Configuration configuration2 = b.build();
      JdbmCacheStoreConfiguration store2 = (JdbmCacheStoreConfiguration) configuration2.loaders().cacheLoaders().get(0);
      assert store2.location().equals("/tmp/jdbm");
      assert store2.expiryQueueSize() == 100;
      assert store2.fetchPersistentState();
      assert store2.async().enabled();

      JdbmCacheStoreConfig legacy = store.adapt();
      assert legacy.getLocation().equals("/tmp/jdbm");
      assert legacy.getExpiryQueueSize() == 100;
      assert legacy.isFetchPersistentState();
      assert legacy.getAsyncStoreConfig().isEnabled();
   }
}