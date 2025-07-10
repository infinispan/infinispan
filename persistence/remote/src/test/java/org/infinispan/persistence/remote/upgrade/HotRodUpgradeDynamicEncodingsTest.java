package org.infinispan.persistence.remote.upgrade;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Same as {@link HotRodUpgradeEncodingsTest} but using remote store created dynamically.
 *
 * @since 13.0
 */
@Test(groups = "functional", testName = "upgrade.hotrod.HotRodUpgradeDynamicEncodingsTest")
public class HotRodUpgradeDynamicEncodingsTest extends HotRodUpgradeEncodingsTest {

   @Factory
   public Object[] factory() {
      return new Object[]{
            new HotRodUpgradeDynamicEncodingsTest().withStorage(StorageType.HEAP),
            new HotRodUpgradeDynamicEncodingsTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).configuredWith(getConfigurationBuilder()).build();
   }

   @Override
   protected void connectTargetCluster() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      RemoteStoreConfigurationBuilder store = configurationBuilder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
      store.remoteCacheName(CACHE_NAME).shared(true).segmented(false).addServer().host("localhost").port(sourceCluster.getHotRodPort()).addProperty(RemoteStore.MIGRATION, "true");

      targetCluster.connectSource(CACHE_NAME, configurationBuilder.build().persistence().stores().get(0));
   }
}
