package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Test;

public class RollingUpgradeTaskTest {

   @Test
   public void testHelloWorldTask() throws Throwable {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(RollingUpgradeTaskTest.class.getName(), "15.2.0.Final", "15.2.1.Final")
            .useCustomServerConfiguration("configuration/ClusteredServerTest.xml")
            .addArchives(ClusteredIT.artifacts())
            .addMavenArtifacts(ClusteredIT.mavenArtifacts())
            .nodeCount(3);

      builder.handlers(ruh -> {
         RemoteCacheManager rcm = ruh.getRemoteCacheManager();
         RemoteCache<String, String> cache = rcm.administration().getOrCreateCache("task-cache",
               new StringConfiguration("<replicated-cache><encoding media-type=\"application/x-protostream\"/></replicated-cache>"));
         Object hello = cache.execute("hello");
         assertThat(hello).isEqualTo("Hello world");
      }, ruh -> {
         RemoteCacheManager rcm = ruh.getRemoteCacheManager();
         RemoteCache<String, String> cache = rcm.getCache("task-cache");
         Object hello = cache.execute("hello");
         assertThat(hello).isEqualTo("Hello world");
         return true;
      });

      RollingUpgradeHandler.performUpgrade(builder.build());
   }
}
