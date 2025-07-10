package org.infinispan.persistence.remote.upgrade;

import java.util.Properties;

import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "upgrade.hotrod.HotRodUpgradeContainersEncodingsTest")
public class HotRodUpgradeContainersEncodingsTest extends HotRodUpgradeEncodingsTest {

   private static final String CONTAINER_NAME = "encoding-container-name";

   @Factory
   @Override
   public Object[] factory() {
      return new Object[] {
            new HotRodUpgradeContainersEncodingsTest().withStorage(StorageType.HEAP),
            new HotRodUpgradeContainersEncodingsTest().withStorage(StorageType.OFF_HEAP),
      };
   }

   @Override
   protected TestCluster configureTargetCluster() {
      return new TestCluster.Builder().setName("targetCluster").setNumMembers(2)
            .cache().name(CACHE_NAME).remotePort(sourceCluster.getHotRodPort())
            .useRemoteContainer(CONTAINER_NAME).configuredWith(getConfigurationBuilder())
            .remoteStoreProperty(RemoteStore.MIGRATION, "true")
            .build(() -> {
               GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
               RemoteContainersConfigurationBuilder rccb = global.addModule(RemoteContainersConfigurationBuilder.class);
               rccb.addRemoteContainer(CONTAINER_NAME)
                     .uri(String.format("hotrod://localhost:%d", sourceCluster.getHotRodPort()));
               return global;
            }, new Properties());
   }
}
