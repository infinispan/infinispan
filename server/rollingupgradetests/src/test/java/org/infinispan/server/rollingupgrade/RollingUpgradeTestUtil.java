package org.infinispan.server.rollingupgrade;

import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_ROLLING_UPGRADE_FROM_VERSION;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_ROLLING_UPGRADE_TO_VERSION;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR;

import java.nio.file.Path;
import java.util.Objects;

import org.infinispan.commons.util.Version;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeVersion;

public class RollingUpgradeTestUtil {
   private static final String INFINISPAN_OLD_VERSION = "infinispan.old.version";

   public static RollingUpgradeVersion getFromVersion() {
      String label = System.getProperty(INFINISPAN_ROLLING_UPGRADE_FROM_VERSION, "latest");
      String version = System.getProperty(INFINISPAN_OLD_VERSION, "16.0.3");
      return new RollingUpgradeVersion(label, version);
   }

   public static RollingUpgradeVersion getToVersion() {
      String toVersion = System.getProperty(INFINISPAN_ROLLING_UPGRADE_TO_VERSION);
      if (toVersion != null) {
         return new RollingUpgradeVersion(toVersion, toVersion);
      }
      // This variable by default is set to the SNAPSHOT test directory in the pom.xml, but can be overridden if needed
      String testServerDir = System.getProperty(INFINISPAN_TEST_SERVER_DIR);
      Objects.requireNonNull(testServerDir, INFINISPAN_TEST_SERVER_DIR + " or " +
            INFINISPAN_ROLLING_UPGRADE_TO_VERSION + " must be defined in the system properties.");
      return new RollingUpgradeVersion("file://" + Path.of(testServerDir).normalize(), Version.getVersion());
   }
}
