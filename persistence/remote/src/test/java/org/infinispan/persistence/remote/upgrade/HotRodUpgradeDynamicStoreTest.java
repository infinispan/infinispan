package org.infinispan.persistence.remote.upgrade;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Same as {@link HotRodUpgradeSynchronizerTest} but using remote store created dynamically.
 *
 * @since 13.0
 */
@Test(testName = "upgrade.hotrod.HotRodUpgradeDynamicStoreTest", groups = "functional")
public class HotRodUpgradeDynamicStoreTest extends HotRodUpgradeSynchronizerTest {

   @Override
   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(OLD_CACHE)
            .cache().name(TEST_CACHE).build();
   }

   @Override
   protected void connectTargetCluster() {
      targetCluster.connectSource(OLD_CACHE, getConfiguration(OLD_CACHE, OLD_PROTOCOL_VERSION));
      targetCluster.connectSource(TEST_CACHE, getConfiguration(TEST_CACHE, NEW_PROTOCOL_VERSION));
   }

   private StoreConfiguration getConfiguration(String cacheName, ProtocolVersion version) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      RemoteStoreConfigurationBuilder storeBuilder = builder.persistence().addStore(RemoteStoreConfigurationBuilder.class);
      storeBuilder.remoteCacheName(cacheName).protocolVersion(version).shared(true).segmented(false)
            .addServer().host("localhost").port(sourceCluster.getHotRodPort());
      return storeBuilder.build().persistence().stores().get(0);
   }
}
