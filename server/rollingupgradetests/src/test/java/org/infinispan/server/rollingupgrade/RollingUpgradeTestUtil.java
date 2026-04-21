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
      String version = System.getProperty(INFINISPAN_OLD_VERSION);
      if (version == null) {
         version = extractVersionFromImageLabel(label);
      }
      if (version == null) {
         version = "16.0.4";
      }
      return new RollingUpgradeVersion(label, version);
   }

   public static RollingUpgradeVersion getToVersion() {
      String toVersion = System.getProperty(INFINISPAN_ROLLING_UPGRADE_TO_VERSION);
      if (toVersion != null) {
         String version = extractVersionFromImageLabel(toVersion);
         if (version == null) {
            version = toVersion.startsWith("image://") ? Version.getVersion() : toVersion;
         }
         return new RollingUpgradeVersion(toVersion, version);
      }
      // This variable by default is set to the SNAPSHOT test directory in the pom.xml, but can be overridden if needed
      String testServerDir = System.getProperty(INFINISPAN_TEST_SERVER_DIR);
      Objects.requireNonNull(testServerDir, INFINISPAN_TEST_SERVER_DIR + " or " +
            INFINISPAN_ROLLING_UPGRADE_TO_VERSION + " must be defined in the system properties.");
      return new RollingUpgradeVersion("file://" + Path.of(testServerDir).normalize(), Version.getVersion());
   }

   /**
    * Extracts a version tag from an image label of the form {@code image://registry/repo:version@sha256:digest}.
    * Returns {@code null} if the label is not an image reference or has no tag.
    */
   static String extractVersionFromImageLabel(String label) {
      if (label == null || !label.startsWith("image://")) {
         return null;
      }
      String s = label.substring("image://".length());
      // Strip digest
      int atIdx = s.indexOf('@');
      if (atIdx >= 0) {
         s = s.substring(0, atIdx);
      }
      // Find tag: colon after the last slash (to avoid matching port numbers)
      int slashIdx = s.lastIndexOf('/');
      int colonIdx = s.indexOf(':', slashIdx >= 0 ? slashIdx : 0);
      if (colonIdx >= 0) {
         return s.substring(colonIdx + 1);
      }
      return null;
   }
}
