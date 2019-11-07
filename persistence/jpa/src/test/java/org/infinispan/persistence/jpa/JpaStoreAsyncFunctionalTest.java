package org.infinispan.persistence.jpa;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jpa.configuration.JpaStoreConfigurationBuilder;
import org.infinispan.persistence.jpa.entity.KeyValueEntity;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.JpaStoreAsyncFunctionalTest")
public class JpaStoreAsyncFunctionalTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(TestingUtil.tmpDirectory(this.getClass()));
      global.serialization().addContextInitializer(JpaSCI.INSTANCE);
      return TestCacheManagerFactory.newDefaultCacheManager(false, global, new ConfigurationBuilder());
   }

   private JpaStoreConfigurationBuilder createJpaConfig() {
      return new ConfigurationBuilder().persistence()
         .addStore(JpaStoreConfigurationBuilder.class)
            .persistenceUnitName("org.infinispan.persistence.jpa")
            .entityClass(KeyValueEntity.class)
            .segmented(false);
   }

   public void testAsyncWriteAndDelete() {
      cacheManager.defineConfiguration("ASYNC_STORE", createJpaConfig().async().enable().build());
      cacheManager.defineConfiguration("SYNC_STORE", createJpaConfig().build());
      Cache<String, KeyValueEntity> asyncStore = cacheManager.getCache("ASYNC_STORE");
      JpaStore<String, KeyValueEntity> syncStore = TestingUtil.getFirstWriter(cacheManager.getCache("SYNC_STORE"));

      String key = "1";
      asyncStore.put(key, new KeyValueEntity(key, "Example"));
      eventually(() -> syncStore.contains(key));
      asyncStore.remove(key);
      eventually(() -> !syncStore.contains(key));
   }
}
