package org.infinispan.server.rollingupgrade;

import java.nio.file.Path;
import java.util.Objects;

import org.infinispan.server.test.core.TestSystemPropertyNames;

public class RollingUpgradeTestUtil {
   public static String getFromVersion() {
      return "16.0.0.Dev02";
   }

   public static String getToVersion() {
      String testServerDir = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
      Objects.requireNonNull(testServerDir, TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR + " must be defined in the system properties.");
      return "file://" + Path.of(testServerDir).normalize();
   }
}
