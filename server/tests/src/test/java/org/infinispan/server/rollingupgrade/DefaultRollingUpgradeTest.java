package org.infinispan.server.rollingupgrade;

import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.junit.jupiter.api.Test;

public class DefaultRollingUpgradeTest {
   @Test
   public void testDefaultSetting() throws InterruptedException {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder("15.2.0.Final", "15.2.1.Final");
      RollingUpgradeHandler.performUpgrade(builder.build());
   }
}
