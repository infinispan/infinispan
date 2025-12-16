package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.artifacts.Artifacts;
import org.infinispan.server.test.core.compatibility.Compatibility;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

public class RollingUpgradeTaskTestIT {

   @Test
   public void testHelloWorldTask() throws Throwable {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(RollingUpgradeTaskTestIT.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .useCustomServerConfiguration("configuration/ClusteredServerTest.xml")
            .addArchives(Artifacts.artifacts())
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

      RollingUpgradeConfiguration configuration = builder.build();
      Assumptions.assumeFalse(Compatibility.INSTANCE.isCompatibilitySkip(configuration));
      RollingUpgradeHandler.performUpgrade(configuration);
   }
}
