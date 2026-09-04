package org.infinispan.persistence.remote.upgrade;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Same as {@link HotRodUpgradeWithStoreTest} but using remote store created dynamically.
 *
 * @since 13.0
 */
@Test(testName = "upgrade.hotrod.HotRodUpgradeDynamicWithStoreTest", groups = "functional")
public class HotRodUpgradeDynamicWithStoreTest extends HotRodUpgradeWithStoreTest {

   protected TestCluster configureTargetCluster() {
      ConfigurationBuilder targetStoreBuilder = new ConfigurationBuilder();
      targetStoreBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).storeName("targetStore");

      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).configuredWith(targetStoreBuilder)
            .build();
   }

   protected void connectTargetCluster() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      RemoteStoreConfigurationBuilder storeBuilder = builder.persistence().addStore(RemoteStoreConfigurationBuilder.class);

      storeBuilder.shared(true).segmented(false).remoteCacheName(CACHE_NAME).addServer()
            .host("localhost").port(sourceCluster.getHotRodPort());

      targetCluster.connectSource(CACHE_NAME, builder.build().persistence().stores().get(0));
   }

}
