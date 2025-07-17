package org.infinispan.server.rollingupgrade;

import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_ROLLING_UPGRADE_FROM_VERSION;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_ROLLING_UPGRADE_TO_VERSION;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR;

import java.nio.file.Path;
import java.util.Objects;

public class RollingUpgradeTestUtil {
   public static String getFromVersion() {
      return System.getProperty(INFINISPAN_ROLLING_UPGRADE_FROM_VERSION, "16.0.0.Dev02");
   }

   public static String getToVersion() {
      String toVersion = System.getProperty(INFINISPAN_ROLLING_UPGRADE_TO_VERSION);
      if (toVersion != null) {
         return toVersion;
      }
      // This variable by default is set to the SNAPSHOT test directory in the pom.xml, but can be overridden if needed
      String testServerDir = System.getProperty(INFINISPAN_TEST_SERVER_DIR);
      Objects.requireNonNull(testServerDir, INFINISPAN_TEST_SERVER_DIR + " or " +
            INFINISPAN_ROLLING_UPGRADE_TO_VERSION + " must be defined in the system properties.");
      return "file://" + Path.of(testServerDir).normalize();
   }
}
