package org.infinispan.server.rollingupgrade;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.test.core.compatibility.Compatibility;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests rolling back from a new version to an old version.
 * <p>
 * Swaps the FROM/TO versions so the rolling upgrade machinery performs a downgrade,
 * verifying that the cluster remains operational throughout. This catches wire format
 * and protocol incompatibilities in the rollback direction.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/17621">ISPN-17621</a>
 */
public class RollingUpgradeRollbackIT {

   @Test
   public void testRollback() {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(
            RollingUpgradeRollbackIT.class.getName(),
            RollingUpgradeTestUtil.getToVersion(),
            RollingUpgradeTestUtil.getFromVersion());
      RollingUpgradeConfiguration configuration = builder
            .useCustomServerConfiguration("configuration/ClusteredServerTest.xml")
            .jgroupsProtocol("test-tcp")
            .nodeCount(3)
            .sharedDataMount(false)
            .handlers(
                  handler -> {
                     RemoteCacheManager rcm = handler.getRemoteCacheManager();
                     RemoteCache<String, String> cache = rcm.administration().createCache("rolling-upgrade",
                           new StringConfiguration("<replicated-cache></replicated-cache>"));
                     cache.put("foo", "bar");
                  },
                  RollingUpgradeConfigurationBuilder.hotrodPredicate("rolling-upgrade",
                        (handler, cache) -> "bar".equals(cache.get("foo")))
            )
            .build();
      Assumptions.assumeFalse(Compatibility.INSTANCE.isCompatibilitySkip(configuration), "Incompatible test");
      RollingUpgradeHandler.performUpgrade(configuration);
   }
}
