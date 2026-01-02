package org.infinispan.persistence.remote.upgrade;

import java.util.Properties;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "upgrade.hotrod.HotRodUpgradeContainerStoreTest", groups = "functional")
public class HotRodUpgradeContainerStoreTest extends HotRodUpgradeWithStoreTest {

   private static final String CONTAINER_NAME = "upgrade-container-name";

   @Override
   protected TestCluster configureTargetCluster() {
      ConfigurationBuilder targetStoreBuilder = new ConfigurationBuilder();
      targetStoreBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).storeName("targetStore");

      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort()).useRemoteContainer(CONTAINER_NAME).remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .configuredWith(targetStoreBuilder)
            .build(() -> {
               GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
               RemoteContainersConfigurationBuilder rccb = global.addModule(RemoteContainersConfigurationBuilder.class);
               rccb.addRemoteContainer(CONTAINER_NAME)
                     .uri(String.format("hotrod://localhost:%d", sourceCluster.getHotRodPort()));
               return global;
            }, new Properties());
   }
}
